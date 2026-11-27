//! A fake Azure AI Speech: the Speech SDK WebSocket protocol, as `src/speech.rs` speaks it.
//!
//! It accepts `/stt/speech/universal/v2` (and the one-language
//! `/speech/recognition/conversation/cognitiveservices/v1`), records the handshake (path, query,
//! headers), `speech.config`, every `speech.context` and the audio frames (the RIFF header first,
//! then PCM), and answers with `turn.start`, `speech.hypothesis`, `speech.phrase` (optionally with
//! word confidences) and `turn.end` written in advance. It can cut connections in mid-stream. What it received is in `calls()` and
//! on `GET /calls` of its HTTP side.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::Router;
use axum::extract::State;
use axum::routing::get;
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;

/// Partial transcript sent by the fake. Unique words, so a test can look for them in logs.
pub const PARTIAL: &str = "Quokka eleven";
/// Final transcript sent by the fake for every utterance.
pub const TRANSCRIPT: &str = "Quokka eleven seventy one walks home";

/// PCM frames between two utterances (25 frames of 40 ms: one second).
const FRAMES_PER_UTTERANCE: usize = 25;

/// The final the fake sends for every utterance.
#[derive(Clone, Debug)]
pub struct Phrase {
    pub status: &'static str,
    pub confidence: f64,
    /// `PrimaryLanguage.Language`, omitted when `None`.
    pub language: Option<&'static str>,
    /// Confidence of each word of `NBest[0].Words`, as Azure sends them in continuous language
    /// detection; no `Words` when `None`. `Some(None)` entries are words without a confidence.
    pub words: Option<Vec<Option<f64>>>,
}

impl Default for Phrase {
    fn default() -> Self {
        Self {
            status: "Success",
            confidence: 0.93,
            language: Some("en-US"),
            words: None,
        }
    }
}

/// One text message as received: its headers (names lower-cased) and its body.
#[derive(Clone, Debug, Default, Serialize)]
pub struct TextMessage {
    pub headers: BTreeMap<String, String>,
    pub body: Value,
}

/// One WebSocket connection as received.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Connection {
    pub path: String,
    pub query: String,
    /// Handshake headers, names lower-cased.
    pub headers: BTreeMap<String, String>,
    pub config: Option<TextMessage>,
    /// One per turn.
    pub contexts: Vec<TextMessage>,
    /// Headers of the first audio message, names lower-cased.
    pub first_audio_headers: BTreeMap<String, String>,
    /// Whether the first audio message carried a RIFF header of 16 kHz mono 16-bit PCM.
    pub riff: bool,
    /// Audio messages carrying the wave header (one per turn).
    pub wave_headers: usize,
    /// Audio messages carrying PCM, and their bytes.
    pub audio_frames: usize,
    pub audio_bytes: usize,
    /// Binary frames that did not follow the framing (length, headers, data).
    pub malformed: usize,
    /// Whether the client sent the empty audio message that ends the stream.
    pub end_of_audio: bool,
    /// Whether the connection is over.
    pub closed: bool,
    /// Whether the fake cut this connection itself.
    pub cut: bool,
}

/// Everything the fake received, one entry per connection.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Calls {
    pub connections: Vec<Connection>,
}

#[derive(Default)]
struct Inner {
    calls: Calls,
    phrase: Phrase,
    /// Connections still to cut, each right after the next final it sends.
    cut_next: usize,
    /// Turns still to end, each right after the next final, as the service does after a while.
    end_turn_next: usize,
}

#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Inner>>);

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Handle on a running fake. The servers stop when the test's runtime ends.
#[derive(Clone)]
pub struct FakeSpeech {
    shared: Shared,
    ws_addr: SocketAddr,
    http_addr: SocketAddr,
}

impl FakeSpeech {
    pub async fn start() -> Self {
        let shared = Shared::default();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws.local_addr().unwrap();
        tokio::spawn(accept(ws, shared.clone()));

        let app = Router::new()
            .route("/calls", get(replay))
            .with_state(shared.clone());
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(http, app).await.unwrap() });
        Self {
            shared,
            ws_addr,
            http_addr,
        }
    }

    /// Base URL to pass as `AZURE_SPEECH_URL`.
    pub fn url(&self) -> String {
        format!("ws://{}", self.ws_addr)
    }

    /// URL of the replay route.
    pub fn calls_url(&self) -> String {
        format!("http://{}/calls", self.http_addr)
    }

    pub fn calls(&self) -> Calls {
        self.shared.lock().calls.clone()
    }

    pub fn connections(&self) -> Vec<Connection> {
        self.calls().connections
    }

    /// Sets the final sent for every utterance from now on.
    pub fn set_phrase(&self, phrase: Phrase) {
        self.shared.lock().phrase = phrase;
    }

    /// Ends the turn after each of the next `count` finals, as the service does after a while:
    /// the client must open a new turn on the same connection.
    pub fn end_turn_next(&self, count: usize) {
        self.shared.lock().end_turn_next = count;
    }

    /// Cuts the next `count` connections, without a closing handshake, each right after the next
    /// final it sends.
    pub fn cut_next(&self, count: usize) {
        self.shared.lock().cut_next = count;
    }
}

async fn replay(State(shared): State<Shared>) -> axum::Json<Calls> {
    axum::Json(shared.lock().calls.clone())
}

async fn accept(listener: TcpListener, shared: Shared) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(session(stream, shared.clone()));
    }
}

fn lower_headers<'a>(
    headers: impl Iterator<Item = (&'a str, &'a str)>,
) -> BTreeMap<String, String> {
    headers
        .map(|(k, v)| (k.to_ascii_lowercase(), v.to_owned()))
        .collect()
}

fn parse_headers(raw: &str) -> BTreeMap<String, String> {
    lower_headers(raw.split("\r\n").filter_map(|line| {
        let (name, value) = line.split_once(':')?;
        Some((name.trim(), value.trim()))
    }))
}

fn answer(path: &str, request_id: &str, body: &Value) -> Message {
    Message::text(format!(
        "Path: {path}\r\nX-RequestId: {request_id}\r\nContent-Type: application/json; charset=utf-8\r\n\r\n{body}"
    ))
}

fn valid_riff(data: &[u8]) -> bool {
    data.len() == 44
        && &data[0..4] == b"RIFF"
        && &data[8..16] == b"WAVEfmt "
        && data[20..22] == 1u16.to_le_bytes()
        && data[22..24] == 1u16.to_le_bytes()
        && data[24..28] == 16_000u32.to_le_bytes()
        && data[34..36] == 16u16.to_le_bytes()
        && &data[36..40] == b"data"
}

async fn session(stream: tokio::net::TcpStream, shared: Shared) {
    let mut connection = Connection::default();
    #[expect(
        clippy::result_large_err,
        reason = "the callback signature is fixed by tungstenite"
    )]
    let callback = |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
        let path = request.uri().path();
        if path != "/stt/speech/universal/v2"
            && path != "/speech/recognition/conversation/cognitiveservices/v1"
        {
            let mut refused = ErrorResponse::new(None);
            *refused.status_mut() = StatusCode::NOT_FOUND;
            return Err(refused);
        }
        connection.path = path.to_owned();
        connection.query = request.uri().query().unwrap_or_default().to_owned();
        connection.headers = lower_headers(
            request
                .headers()
                .iter()
                .filter_map(|(k, v)| Some((k.as_str(), v.to_str().ok()?))),
        );
        Ok(response)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let index = {
        let mut inner = shared.lock();
        inner.calls.connections.push(connection);
        inner.calls.connections.len() - 1
    };
    let update = |f: &mut dyn FnMut(&mut Connection)| {
        let mut inner = shared.lock();
        if let Some(c) = inner.calls.connections.get_mut(index) {
            f(c);
        }
    };
    let (mut sink, mut stream) = ws.split();
    let mut request_id = String::new();
    let mut frames = 0usize;
    let mut turn_started = false;
    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Text(text) => {
                let (head, body) = text.as_str().split_once("\r\n\r\n").unwrap_or_default();
                let headers = parse_headers(head);
                let body: Value = serde_json::from_str(body).unwrap_or(Value::Null);
                let path = headers.get("path").cloned().unwrap_or_default();
                request_id = headers.get("x-requestid").cloned().unwrap_or_default();
                let message = TextMessage { headers, body };
                match path.as_str() {
                    "speech.config" => update(&mut |c| c.config = Some(message.clone())),
                    "speech.context" => {
                        turn_started = false;
                        update(&mut |c| c.contexts.push(message.clone()));
                    }
                    _ => {}
                }
            }
            Message::Binary(bytes) => {
                let Some((headers, data)) = split_binary(&bytes) else {
                    update(&mut |c| c.malformed += 1);
                    continue;
                };
                if headers.get("path").map(String::as_str) != Some("audio") {
                    update(&mut |c| c.malformed += 1);
                    continue;
                }
                if headers.get("content-type").map(String::as_str) == Some("audio/x-wav") {
                    let riff = valid_riff(data);
                    update(&mut |c| {
                        if c.wave_headers == 0 {
                            c.first_audio_headers = headers.clone();
                            c.riff = riff;
                        }
                        c.wave_headers += 1;
                    });
                    if !turn_started {
                        turn_started = true;
                        let body = json!({ "context": { "serviceTag": "fake" } });
                        if sink
                            .send(answer("turn.start", &request_id, &body))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    continue;
                }
                if data.is_empty() {
                    update(&mut |c| c.end_of_audio = true);
                    let end = [
                        answer("speech.endDetected", &request_id, &json!({ "Offset": 0 })),
                        answer("turn.end", &request_id, &json!({})),
                    ];
                    for message in end {
                        let _ = sink.send(message).await;
                    }
                    continue;
                }
                update(&mut |c| {
                    c.audio_frames += 1;
                    c.audio_bytes += data.len();
                });
                if frames.is_multiple_of(FRAMES_PER_UTTERANCE) {
                    let phrase = shared.lock().phrase.clone();
                    for message in script(&request_id, &phrase) {
                        if sink.send(message).await.is_err() {
                            break;
                        }
                    }
                    let end_turn = {
                        let mut inner = shared.lock();
                        let end = inner.end_turn_next > 0;
                        if end {
                            inner.end_turn_next -= 1;
                        }
                        end
                    };
                    if end_turn
                        && sink
                            .send(answer("turn.end", &request_id, &json!({})))
                            .await
                            .is_err()
                    {
                        break;
                    }
                    let cut = {
                        let mut inner = shared.lock();
                        let cut = inner.cut_next > 0;
                        if cut {
                            inner.cut_next -= 1;
                        }
                        cut
                    };
                    if cut {
                        update(&mut |c| {
                            c.cut = true;
                            c.closed = true;
                        });
                        // Dropping both halves closes the TCP connection without a close frame.
                        return;
                    }
                }
                frames += 1;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    update(&mut |c| c.closed = true);
}

/// Splits a binary frame: 2 bytes of header length (big-endian), the headers, the data.
fn split_binary(bytes: &[u8]) -> Option<(BTreeMap<String, String>, &[u8])> {
    let length = usize::from(u16::from_be_bytes([*bytes.first()?, *bytes.get(1)?]));
    let headers = std::str::from_utf8(bytes.get(2..2 + length)?).ok()?;
    Some((parse_headers(headers), bytes.get(2 + length..)?))
}

fn script(request_id: &str, phrase: &Phrase) -> Vec<Message> {
    let language = phrase
        .language
        .map(|l| json!({ "Language": l, "Confidence": "High" }));
    let mut hypothesis = json!({ "Text": PARTIAL, "Offset": 1_000_000, "Duration": 5_000_000 });
    let mut final_phrase = json!({
        "RecognitionStatus": phrase.status,
        "Offset": 1_000_000,
        "Duration": 9_000_000,
        "DisplayText": TRANSCRIPT,
        "NBest": [{
            "Confidence": phrase.confidence,
            "Lexical": TRANSCRIPT.to_lowercase(),
            "ITN": TRANSCRIPT.to_lowercase(),
            "MaskedITN": TRANSCRIPT.to_lowercase(),
            "Display": TRANSCRIPT
        }]
    });
    if let Some(words) = &phrase.words {
        let words: Vec<Value> = TRANSCRIPT
            .split(' ')
            .cycle()
            .zip(words)
            .enumerate()
            .map(|(i, (word, confidence))| {
                let mut word = json!({
                    "Word": word.to_lowercase(),
                    "Offset": 1_000_000 + i * 1_000_000,
                    "Duration": 900_000
                });
                if let Some(confidence) = confidence {
                    word["Confidence"] = json!(confidence);
                }
                word
            })
            .collect();
        final_phrase["NBest"][0]["Words"] = Value::Array(words);
    }
    if let Some(language) = language {
        hypothesis["PrimaryLanguage"] = language.clone();
        final_phrase["PrimaryLanguage"] = language;
    }
    vec![
        answer(
            "speech.startDetected",
            request_id,
            &json!({ "Offset": 1_000_000 }),
        ),
        answer("speech.hypothesis", request_id, &hypothesis),
        answer("speech.phrase", request_id, &final_phrase),
    ]
}
