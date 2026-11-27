//! Azure AI Speech real-time transcription, one WebSocket per speaker.
//!
//! Azure publishes no Rust SDK and no public page for this protocol (the old "Speech WebSocket
//! protocol" page is gone). Every message below follows the official Speech SDK for JavaScript
//! (MIT), <https://github.com/microsoft/cognitive-services-speech-sdk-js>, at commit
//! [`SDK_REFERENCE`] (tag v1.53.0). Each item names the file of `src/` it comes from. The
//! Azure-Samples guide "How to Use Azure Speech Translation via WebSocket (No SDK)"
//! (`Azure-Samples/voice-translator-and-personal-voice`, commit
//! `67b13333a6132db71e812bf59448fef123cdbeb3`, `HOW_TO_USE_SPEECH_TRANSLATION_WEBSOCKETS.md`)
//! describes the same framing from the SDK's logs.
//!
//! If Microsoft breaks the protocol, the fallback is the official C SDK called through FFI, still
//! one process (see the README); the canary test (`tests/canary.rs`) is there to notice.

use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing::{debug, info, warn};

use crate::audio::SAMPLE_RATE;

/// Commit of the Speech SDK for JavaScript the protocol below is taken from (tag v1.53.0).
pub const SDK_REFERENCE: &str = "5a37ff77fa8ba874a9f7a29bcf2bf34acda89d77";

/// Path of continuous recognition with language detection.
///
/// `src/common.speech/SpeechConnectionFactory.ts`: `universalUri` plus
/// `recognitionEndpointVersion` (`"2"`, `RecognizerConfig.ts`). The older
/// `/speech/recognition/conversation/cognitiveservices/v1` takes one language only.
pub const PATH: &str = "/stt/speech/universal/v2";

/// Most locales one speaker may declare: Azure detects among 10 at most in continuous mode.
pub const MAX_LOCALES: usize = 10;

/// Default `AZURE_SPEECH_MIN_CONFIDENCE`: finals below it are suspect. Set on the fake, to be
/// measured on real calls.
pub const DEFAULT_MIN_CONFIDENCE: f32 = 0.5;

/// How long to wait for the last results after the end of the audio.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
/// How long the WebSocket handshake may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Failed connections in a row before a speaker's transcription is given up.
const RECONNECT_ATTEMPTS: u32 = 8;
const FIRST_RECONNECT_DELAY: Duration = Duration::from_millis(500);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(8);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptionKind {
    Partial,
    Final,
    Translation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CaptionEvent {
    /// Utterance id: a partial, its final and its translation share it.
    pub utterance_id: String,
    pub kind: CaptionKind,
    pub text: String,
    /// Source text, for translations.
    pub original: Option<String>,
    /// Locale detected by Azure for this utterance (or the speaker's only locale).
    pub language: Option<String>,
    pub confidence: Option<f32>,
    /// Set on finals Azure did not recognize, recognized with a low confidence, or in a language
    /// the speaker did not declare: they never reach the listeners.
    pub suspect: bool,
}

/// Azure AI Speech client: where to connect, with which key.
#[derive(Clone)]
pub struct SpeechClient {
    base_url: String,
    key: String,
    min_confidence: f32,
    first_reconnect_delay: Duration,
}

/// Base URL of Azure AI Speech in `region`, e.g. `wss://francecentral.stt.speech.microsoft.com`.
///
/// `src/common.speech/SpeechConnectionFactory.ts`: `"wss://" + region + ".stt.speech" + hostSuffix`.
pub fn region_url(region: &str) -> String {
    format!("wss://{region}.stt.speech.microsoft.com")
}

impl SpeechClient {
    /// `base_url` is a [`region_url`], or the address of a fake (`AZURE_SPEECH_URL`).
    pub fn new(base_url: &str, key: &str, min_confidence: f32) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            min_confidence,
            first_reconnect_delay: FIRST_RECONNECT_DELAY,
        }
    }

    /// Sets the first delay before reconnecting, doubled at each failure up to 8 s.
    #[must_use]
    pub fn with_first_reconnect_delay(mut self, delay: Duration) -> Self {
        self.first_reconnect_delay = delay;
        self
    }

    /// WebSocket URL for a speaker who speaks `locales`.
    ///
    /// `src/common.speech/SpeechConnectionFactory.ts` and `QueryParameterNames.ts`: `language`
    /// with one locale, `lidEnabled=true` with several, `format=detailed` for the confidence.
    /// Never `storeAudio`: Azure would keep the audio and the transcripts.
    pub fn url(&self, locales: &[String]) -> Result<String> {
        let mut url = reqwest::Url::parse(&format!("{}{PATH}", self.base_url))
            .context("invalid AZURE_SPEECH_URL")?;
        {
            let mut query = url.query_pairs_mut();
            if let [one] = locales {
                query.append_pair("language", one);
            }
            query.append_pair("format", "detailed");
            if locales.len() > 1 {
                query.append_pair("lidEnabled", "true");
            }
            query.append_pair("profanity", "raw");
        }
        Ok(url.to_string())
    }

    /// Streams `audio` (PCM 16 kHz mono 16-bit chunks) to Azure and forwards captions to
    /// `events` until `audio` closes.
    ///
    /// When the service closes the connection or cannot be reached, reconnects with a doubling
    /// delay; the count of failures starts again once a connection got an answer.
    ///
    /// # Errors
    ///
    /// [`RECONNECT_ATTEMPTS`] connections in a row failed.
    pub async fn transcribe(
        &self,
        session: &str,
        locales: &[String],
        mut audio: mpsc::Receiver<Vec<u8>>,
        events: mpsc::Sender<CaptionEvent>,
    ) -> Result<()> {
        let url = self.url(locales)?;
        let mut utterances = Utterances::new(session);
        let mut failures = 0u32;
        loop {
            let connection = new_id();
            match self.connect(&url, &connection).await {
                Ok(ws) => {
                    info!(session, %connection, "Azure Speech connected");
                    let mut stream = Stream {
                        locales,
                        min_confidence: self.min_confidence,
                        utterances: &mut utterances,
                        events: &events,
                        answered: false,
                    };
                    let ended = stream.run(ws, &mut audio).await;
                    if stream.answered {
                        failures = 0;
                    }
                    match ended {
                        Ended::Finished => return Ok(()),
                        Ended::Dropped(why) => {
                            info!(session, %connection, %why, "Azure Speech connection lost");
                        }
                    }
                }
                Err(err) => {
                    warn!(session, error = %format!("{err:#}"), "Azure Speech connection failed");
                }
            }
            failures += 1;
            if failures > RECONNECT_ATTEMPTS {
                bail!("Azure Speech failed {RECONNECT_ATTEMPTS} times in a row, giving up");
            }
            tokio::time::sleep(reconnect_delay(self.first_reconnect_delay, failures)).await;
            if audio.is_closed() && audio.is_empty() {
                return Ok(());
            }
        }
    }

    async fn connect(&self, url: &str, connection: &str) -> Result<Socket> {
        let mut request = url
            .into_client_request()
            .context("invalid Azure Speech URL")?;
        // `src/common.speech/HeaderNames.ts` (`AuthKey`, `ConnectionId`) and
        // `SpeechConnectionFactory.ts`: the key and a random connection id on the handshake.
        let headers = request.headers_mut();
        headers.insert(
            "Ocp-Apim-Subscription-Key",
            HeaderValue::from_str(&self.key).context("AZURE_SPEECH_KEY is not a valid header")?,
        );
        headers.insert(
            "X-ConnectionId",
            HeaderValue::from_str(connection).context("invalid connection id")?,
        );
        let (ws, _) =
            tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .context("Azure Speech did not answer the handshake")?
                .context("Azure Speech unreachable")?;
        Ok(ws)
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// How one connection ended.
enum Ended {
    /// The audio ended and the last results came (or the drain timed out).
    Finished,
    /// The connection dropped while audio was still coming.
    Dropped(String),
}

/// Utterance ids of one speaker: `<session>-<n>`, `n` moving on after each final.
struct Utterances {
    session: String,
    next: u64,
}

impl Utterances {
    fn new(session: &str) -> Self {
        Self {
            session: session.to_owned(),
            next: 1,
        }
    }

    fn current(&self) -> String {
        format!("{}-{}", self.session, self.next)
    }

    fn finish(&mut self) -> String {
        let id = self.current();
        self.next += 1;
        id
    }
}

/// One WebSocket connection's run.
struct Stream<'a> {
    locales: &'a [String],
    min_confidence: f32,
    utterances: &'a mut Utterances,
    events: &'a mpsc::Sender<CaptionEvent>,
    /// Set once the service sent anything on this connection.
    answered: bool,
}

impl Stream<'_> {
    async fn run(&mut self, ws: Socket, audio: &mut mpsc::Receiver<Vec<u8>>) -> Ended {
        let (mut sink, mut stream) = ws.split();
        let mut request = new_id();
        // `src/common.speech/ServiceRecognizerBase.ts`, `configureConnection`: speech.config,
        // then (`sendPrePayloadJSON`) speech.context and the wave header.
        let opening = [
            Message::text(text_message(
                "speech.config",
                &request,
                &speech_config().to_string(),
            )),
            Message::text(text_message(
                "speech.context",
                &request,
                &speech_context(self.locales).to_string(),
            )),
            Message::binary(wave_header_message(&request)),
        ];
        for message in opening {
            if let Err(err) = sink.send(message).await {
                return Ended::Dropped(format!("send failed: {err}"));
            }
        }
        let mut stopping = false;
        loop {
            tokio::select! {
                chunk = audio.recv(), if !stopping => {
                    // `ServiceRecognizerBase.ts`, `sendAudio` and `sendFinalAudio`: an audio
                    // message without a body ends the stream.
                    let body = chunk.as_deref().unwrap_or_default();
                    stopping = chunk.is_none();
                    let sent = sink.send(Message::binary(audio_message(&request, None, body))).await;
                    if let Err(err) = sent {
                        if stopping {
                            return Ended::Finished;
                        }
                        return Ended::Dropped(format!("audio send failed: {err}"));
                    }
                },
                message = next_message(&mut stream, stopping) => match message {
                    Some(Ok(Message::Text(text))) => {
                        self.answered = true;
                        let Some((path, body)) = parse_text_message(text.as_str()) else {
                            debug!("ignored malformed Azure Speech message");
                            continue;
                        };
                        if path.eq_ignore_ascii_case("turn.end") {
                            if stopping {
                                let _ = sink.send(Message::Close(None)).await;
                                return Ended::Finished;
                            }
                            // `ServiceRecognizerBase.ts`, `turn.end` in continuous mode: a new
                            // turn, with a new request id, its speech.context and wave header.
                            request = new_id();
                            let context = text_message(
                                "speech.context",
                                &request,
                                &speech_context(self.locales).to_string(),
                            );
                            let resumed = async {
                                sink.send(Message::text(context)).await?;
                                sink.send(Message::binary(wave_header_message(&request))).await
                            };
                            if let Err(err) = resumed.await {
                                return Ended::Dropped(format!("send failed: {err}"));
                            }
                            continue;
                        }
                        if let Some(event) = self.event(&path, body) {
                            let _ = self.events.send(event).await;
                        }
                    }
                    Some(Ok(Message::Close(_)) | Err(_)) | None if stopping => return Ended::Finished,
                    Some(Ok(Message::Close(frame))) => {
                        let why = frame.map_or_else(
                            || "closed without a code".to_owned(),
                            |f| format!(
                                "closed with code {} ({})",
                                u16::from(f.code),
                                f.reason.chars().take(120).collect::<String>()
                            ),
                        );
                        return Ended::Dropped(why);
                    }
                    None => return Ended::Dropped("stream ended".to_owned()),
                    Some(Err(err)) => return Ended::Dropped(format!("stream error: {err}")),
                    Some(Ok(_)) => {}
                },
            }
        }
    }

    fn event(&mut self, path: &str, body: &str) -> Option<CaptionEvent> {
        match parse_answer(path, body) {
            Ok(answer) => to_event(answer, self.locales, self.min_confidence, self.utterances),
            Err(err) => {
                debug!(%err, "ignored Azure Speech message");
                None
            }
        }
    }
}

async fn next_message(
    stream: &mut futures::stream::SplitStream<Socket>,
    stopping: bool,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    if stopping {
        tokio::time::timeout(DRAIN_TIMEOUT, stream.next())
            .await
            .unwrap_or(None)
    } else {
        stream.next().await
    }
}

/// Delay before reconnection attempt `failures` (1-based): doubling from `first`, at most 8 s.
fn reconnect_delay(first: Duration, failures: u32) -> Duration {
    let factor = 1u32 << failures.saturating_sub(1).min(5);
    first.saturating_mul(factor).min(MAX_RECONNECT_DELAY)
}

/// Parses a speaker's locales: one Azure locale or a comma-separated list, e.g. `fr-FR,en-US`.
///
/// A locale is a language (2 or 3 letters), an optional script (4 letters) and a region (2
/// letters or 3 digits), normalised to `fr-FR` / `sr-Latn-RS`; duplicates are dropped. Returns
/// `None` when the value is empty, any locale is invalid, or there are more than
/// [`MAX_LOCALES`].
pub fn parse_locales(raw: &str) -> Option<Vec<String>> {
    let mut locales: Vec<String> = Vec::new();
    for locale in raw.split(',') {
        let locale = normalize_locale(locale.trim())?;
        if !locales.contains(&locale) {
            locales.push(locale);
        }
    }
    (locales.len() <= MAX_LOCALES).then_some(locales)
}

fn normalize_locale(raw: &str) -> Option<String> {
    let parts: Vec<&str> = raw.split('-').collect();
    let (language, script, region) = match parts.as_slice() {
        [language, region] => (*language, None, *region),
        [language, script, region] => (*language, Some(*script), *region),
        _ => return None,
    };
    let letters = |s: &str, n: std::ops::RangeInclusive<usize>| {
        n.contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphabetic())
    };
    let region_ok =
        letters(region, 2..=2) || (region.len() == 3 && region.bytes().all(|b| b.is_ascii_digit()));
    if !letters(language, 2..=3) || !region_ok || script.is_some_and(|s| !letters(s, 4..=4)) {
        return None;
    }
    let mut out = language.to_ascii_lowercase();
    if let Some(script) = script {
        let lower = script.to_ascii_lowercase();
        let mut chars = lower.chars();
        out.push('-');
        out.extend(chars.next().map(|c| c.to_ascii_uppercase()));
        out.extend(chars);
    }
    out.push('-');
    out.push_str(&region.to_ascii_uppercase());
    Some(out)
}

/// Body of `speech.config`, sent once per connection.
///
/// `src/common.speech/SpeechServiceConfig.ts` (`Context`, `System`, `OS`,
/// `ISpeechConfigAudioDevice`) and `ServiceRecognizerBase.ts` (`sendSpeechServiceConfig`).
pub fn speech_config() -> Value {
    json!({
        "context": {
            "system": {
                "name": "SpeechSDK",
                "version": "1.53.0",
                "build": "livecap",
                "lang": "Rust"
            },
            "os": { "platform": "Linux", "name": "livecap", "version": env!("CARGO_PKG_VERSION") },
            "audio": {
                "source": {
                    "manufacturer": "livecap",
                    "model": "LiveKit track",
                    "connectivity": "Unknown",
                    "type": "Stream",
                    "samplerate": SAMPLE_RATE,
                    "bitspersample": 16,
                    "channelcount": 1
                }
            }
        }
    })
}

/// Body of `speech.context`, sent at the start of every turn.
///
/// `src/common.speech/ServiceRecognizerBase.ts`: `recognize` (`phraseDetection.mode`, here
/// `Conversation`), `setLanguageJson` (`phraseDetection.language` with one locale),
/// `setLanguageIdJson` (`languageId` in `DetectContinuous` mode, with `phraseOutput`, for
/// several), and `SpeechContext.ts` `setDetailedOutputFormat`. No `speakerDiarization` and no
/// speaker identification: livecap never asks who speaks.
pub fn speech_context(locales: &[String]) -> Value {
    let detailed = json!({ "format": "Detailed", "detailed": { "options": [] } });
    if let [one] = locales {
        return json!({
            "phraseDetection": { "mode": "Conversation", "language": one },
            "phraseOutput": detailed
        });
    }
    json!({
        "phraseDetection": { "mode": "Conversation" },
        "languageId": {
            "languages": locales,
            "mode": "DetectContinuous",
            "onSuccess": { "action": "Recognize" },
            "onUnknown": { "action": "None" },
            "priority": "PrioritizeLatency"
        },
        "phraseOutput": {
            "format": "Detailed",
            "detailed": { "options": [] },
            "interimResults": { "resultType": "Auto" },
            "phraseResults": { "resultType": "Always" }
        }
    })
}

/// RIFF header of 16 kHz mono 16-bit PCM, sizes left at zero for a stream.
///
/// `src/sdk/Audio/AudioStreamFormat.ts`, the `header` built in `AudioStreamFormatImpl`.
pub fn riff_header() -> Vec<u8> {
    let mut header = Vec::with_capacity(44);
    header.extend_from_slice(b"RIFF");
    header.extend_from_slice(&0u32.to_le_bytes());
    header.extend_from_slice(b"WAVEfmt ");
    header.extend_from_slice(&16u32.to_le_bytes());
    header.extend_from_slice(&1u16.to_le_bytes()); // PCM
    header.extend_from_slice(&1u16.to_le_bytes()); // mono
    header.extend_from_slice(&SAMPLE_RATE.to_le_bytes());
    header.extend_from_slice(&(SAMPLE_RATE * 2).to_le_bytes()); // bytes per second
    header.extend_from_slice(&2u16.to_le_bytes()); // block align
    header.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    header.extend_from_slice(b"data");
    header.extend_from_slice(&0u32.to_le_bytes());
    header
}

/// A text message: headers, a blank line, the body.
///
/// `src/common.speech/WebsocketMessageFormatter.ts` (`fromConnectionMessage`, `makeHeaders`) and
/// `SpeechConnectionMessage.Internal.ts` (`Path`, `X-RequestId`, `X-Timestamp`, `Content-Type`).
pub fn text_message(path: &str, request_id: &str, body: &str) -> String {
    format!(
        "Path: {path}\r\nX-RequestId: {request_id}\r\nX-Timestamp: {}\r\nContent-Type: application/json\r\n\r\n{body}",
        timestamp()
    )
}

/// A binary message: header length on 2 bytes big-endian, the headers, then the data.
///
/// `src/common.speech/WebsocketMessageFormatter.ts` (`fromConnectionMessage`).
pub fn audio_message(request_id: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut headers = format!(
        "Path: audio\r\nX-RequestId: {request_id}\r\nX-Timestamp: {}\r\n",
        timestamp()
    );
    if let Some(content_type) = content_type {
        headers.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    // The headers are a few dozen ASCII bytes: the length always fits.
    let length = u16::try_from(headers.len()).unwrap_or(u16::MAX);
    let mut message = Vec::with_capacity(2 + headers.len() + body.len());
    message.extend_from_slice(&length.to_be_bytes());
    message.extend_from_slice(headers.as_bytes());
    message.extend_from_slice(body);
    message
}

/// `ServiceRecognizerBase.ts`, `sendWaveHeader`: the first audio message of a turn.
fn wave_header_message(request_id: &str) -> Vec<u8> {
    audio_message(request_id, Some("audio/x-wav"), &riff_header())
}

/// Splits a text message into its `Path` header and its body.
///
/// `src/common.speech/WebsocketMessageFormatter.ts` (`toConnectionMessage`, `parseHeaders`):
/// header names are case-insensitive.
pub fn parse_text_message(raw: &str) -> Option<(String, &str)> {
    let (headers, body) = raw.split_once("\r\n\r\n").unwrap_or((raw, ""));
    let path = headers.split("\r\n").find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case("path")
            .then(|| value.trim().to_owned())
    })?;
    Some((path, body))
}

/// What Azure answered, reduced to what livecap uses.
#[derive(Debug, PartialEq)]
pub enum Answer {
    /// `speech.hypothesis` (or `speech.fragment`): a partial transcript.
    Hypothesis {
        text: String,
        language: Option<String>,
    },
    /// `speech.phrase`: the final transcript of an utterance.
    Phrase {
        status: String,
        text: String,
        confidence: Option<f32>,
        language: Option<String>,
    },
    /// Anything else (`turn.start`, `speech.startDetected`…).
    Other,
}

/// `src/common.speech/ServiceMessages/SpeechHypothesis.ts`.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Hypothesis {
    #[serde(default)]
    text: String,
    primary_language: Option<PrimaryLanguage>,
}

/// `src/common.speech/ServiceMessages/DetailedSpeechPhrase.ts` (and `SimpleSpeechPhrase.ts` for
/// `PrimaryLanguage` and `DisplayText`).
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Phrase {
    #[serde(default)]
    recognition_status: String,
    #[serde(default)]
    display_text: Option<String>,
    #[serde(default, rename = "NBest")]
    n_best: Vec<Best>,
    primary_language: Option<PrimaryLanguage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Best {
    confidence: Option<f32>,
    display: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct PrimaryLanguage {
    language: Option<String>,
}

/// Parses the body of one answer by its path.
///
/// # Errors
///
/// A body that is not the expected JSON. The error never quotes the body: it can hold what
/// someone said, and errors are logged.
pub fn parse_answer(path: &str, body: &str) -> Result<Answer> {
    let path = path.to_ascii_lowercase();
    let malformed = |err: serde_json::Error| {
        anyhow::anyhow!("malformed Azure Speech {path} ({:?})", err.classify())
    };
    match path.as_str() {
        "speech.hypothesis" | "speech.fragment" => {
            let h: Hypothesis = serde_json::from_str(body).map_err(malformed)?;
            Ok(Answer::Hypothesis {
                text: h.text.trim().to_owned(),
                language: h.primary_language.and_then(|l| l.language),
            })
        }
        "speech.phrase" => {
            let p: Phrase = serde_json::from_str(body).map_err(malformed)?;
            let best = p.n_best.into_iter().next();
            let confidence = best.as_ref().and_then(|b| b.confidence);
            let text = best
                .and_then(|b| b.display)
                .or(p.display_text)
                .unwrap_or_default();
            Ok(Answer::Phrase {
                status: p.recognition_status,
                text: text.trim().to_owned(),
                confidence,
                language: p.primary_language.and_then(|l| l.language),
            })
        }
        _ => Ok(Answer::Other),
    }
}

/// Turns an answer into a caption event, or `None` for answers without text.
fn to_event(
    answer: Answer,
    locales: &[String],
    min_confidence: f32,
    utterances: &mut Utterances,
) -> Option<CaptionEvent> {
    // With one locale Azure may not name the language: it is that locale.
    let fallback = || match locales {
        [one] => Some(one.clone()),
        _ => None,
    };
    match answer {
        Answer::Hypothesis { text, language } if !text.is_empty() => Some(CaptionEvent {
            utterance_id: utterances.current(),
            kind: CaptionKind::Partial,
            text,
            original: None,
            language: language.or_else(fallback),
            confidence: None,
            suspect: false,
        }),
        Answer::Phrase {
            status,
            text,
            confidence,
            language,
        } if !text.is_empty() => {
            let language = language.or_else(fallback);
            let declared = language
                .as_deref()
                .is_some_and(|l| locales.iter().any(|d| d.eq_ignore_ascii_case(l)));
            // A final without a confidence is not trusted: with one locale the threshold is the
            // only guard against an uncovered language transcribed as the declared one.
            let confident = confidence.is_some_and(|c| c >= min_confidence);
            // `src/common.speech/ServiceMessages/Enums.ts`, `RecognitionStatus`.
            let suspect = status != "Success" || !confident || !declared;
            Some(CaptionEvent {
                utterance_id: utterances.finish(),
                kind: CaptionKind::Final,
                text,
                original: None,
                language,
                confidence,
                suspect,
            })
        }
        Answer::Phrase { status, .. } if status != "Success" && is_service_error(&status) => {
            warn!(%status, "Azure Speech reported an error");
            None
        }
        _ => None,
    }
}

/// `RecognitionStatus` values that mean the request failed rather than heard nothing.
fn is_service_error(status: &str) -> bool {
    matches!(
        status,
        "Error" | "BadRequest" | "Forbidden" | "TooManyRequests"
    )
}

/// A random id of 32 upper-case hex digits, the form of the SDK's `createNoDashGuid`
/// (`src/common/Guid.ts`).
pub fn new_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut id = String::with_capacity(32);
    for _ in 0..2 {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
        hasher.write_u128(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        );
        id.push_str(&format!("{:016X}", hasher.finish()));
    }
    id
}

/// Current UTC time as ISO 8601 with milliseconds, like JavaScript's `toISOString`.
fn timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let (days, rest) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60,
        now.subsec_millis()
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        Answer, CaptionKind, SpeechClient, Utterances, audio_message, civil_from_days, new_id,
        parse_answer, parse_locales, parse_text_message, reconnect_delay, riff_header,
        speech_context, text_message, to_event,
    };

    fn locales(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|l| (*l).to_owned()).collect()
    }

    #[test]
    fn locales_are_one_azure_locale_or_a_comma_separated_list() {
        assert_eq!(parse_locales("fr-FR"), Some(locales(&["fr-FR"])));
        assert_eq!(
            parse_locales(" fr-fr, EN-us ,fr-FR"),
            Some(locales(&["fr-FR", "en-US"]))
        );
        assert_eq!(parse_locales("sr-latn-rs"), Some(locales(&["sr-Latn-RS"])));
        assert_eq!(parse_locales("fil-PH"), Some(locales(&["fil-PH"])));
    }

    #[test]
    fn empty_invalid_or_too_long_locale_lists_are_refused() {
        for raw in [
            "",
            " ",
            "fr",
            "fr-FR,",
            "fr-FR,,en-US",
            "f-FR",
            "fren-FR",
            "fr-F",
            "fr_FR",
            "12-FR",
            "fr-FR;en-US",
            "fr-Latin-FR",
            "fr-FR-x",
        ] {
            assert_eq!(parse_locales(raw), None, "{raw:?}");
        }
        let ten = "ar-EG,de-DE,en-US,es-ES,fr-FR,it-IT,ja-JP,ko-KR,pt-BR,zh-CN";
        assert_eq!(parse_locales(ten).map(|l| l.len()), Some(10));
        assert_eq!(parse_locales(&format!("{ten},ru-RU")), None);
    }

    #[test]
    fn the_url_names_the_locale_or_turns_on_language_detection() -> anyhow::Result<()> {
        let client = SpeechClient::new("wss://francecentral.stt.speech.microsoft.com/", "k", 0.5);
        assert_eq!(
            client.url(&locales(&["fr-FR"]))?,
            "wss://francecentral.stt.speech.microsoft.com/stt/speech/universal/v2?language=fr-FR&format=detailed&profanity=raw"
        );
        assert_eq!(
            client.url(&locales(&["fr-FR", "en-US"]))?,
            "wss://francecentral.stt.speech.microsoft.com/stt/speech/universal/v2?format=detailed&lidEnabled=true&profanity=raw"
        );
        Ok(())
    }

    #[test]
    fn the_context_never_asks_for_diarization_or_speaker_identification() {
        for list in [locales(&["fr-FR"]), locales(&["fr-FR", "en-US"])] {
            let context = speech_context(&list).to_string().to_lowercase();
            for word in ["diarization", "speaker", "storeaudio", "transcriber"] {
                assert!(!context.contains(word), "{word} in {context}");
            }
        }
    }

    #[test]
    fn text_messages_have_headers_a_blank_line_and_the_body() {
        let message = text_message("speech.config", "ABC", "{}");
        let (headers, body) = message.split_once("\r\n\r\n").unwrap_or_default();
        assert_eq!(body, "{}");
        let lines: Vec<&str> = headers.split("\r\n").collect();
        assert_eq!(lines[0], "Path: speech.config");
        assert_eq!(lines[1], "X-RequestId: ABC");
        assert!(lines[2].starts_with("X-Timestamp: 20"), "{}", lines[2]);
        assert!(lines[2].ends_with('Z'), "{}", lines[2]);
        assert_eq!(lines[3], "Content-Type: application/json");
        assert_eq!(
            parse_text_message("path: turn.end\r\nX-RequestId: A\r\n\r\n{}"),
            Some(("turn.end".to_owned(), "{}"))
        );
    }

    #[test]
    fn audio_messages_carry_their_header_length_big_endian() {
        let message = audio_message("ABC", Some("audio/x-wav"), &[1, 2]);
        let length = usize::from(u16::from_be_bytes([message[0], message[1]]));
        let headers = std::str::from_utf8(&message[2..2 + length]).unwrap_or_default();
        assert!(headers.starts_with("Path: audio\r\nX-RequestId: ABC\r\n"));
        assert!(headers.ends_with("Content-Type: audio/x-wav\r\n"));
        assert_eq!(&message[2 + length..], &[1, 2]);
        let end = audio_message("ABC", None, &[]);
        let length = usize::from(u16::from_be_bytes([end[0], end[1]]));
        assert_eq!(end.len(), 2 + length);
    }

    #[test]
    fn the_riff_header_describes_16_khz_mono_16_bit_pcm() {
        let header = riff_header();
        assert_eq!(header.len(), 44);
        assert_eq!(&header[0..4], b"RIFF");
        assert_eq!(&header[8..16], b"WAVEfmt ");
        assert_eq!(
            u32::from_le_bytes([header[24], header[25], header[26], header[27]]),
            16_000
        );
        assert_eq!(u16::from_le_bytes([header[22], header[23]]), 1);
        assert_eq!(u16::from_le_bytes([header[34], header[35]]), 16);
        assert_eq!(&header[36..40], b"data");
    }

    #[test]
    fn ids_are_32_upper_case_hex_digits_and_differ() {
        let (a, b) = (new_id(), new_id());
        assert_eq!(a.len(), 32);
        assert!(
            a.bytes()
                .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
        );
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    #[test]
    fn dates_are_computed_from_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(20_784), (2026, 11, 27));
    }

    #[test]
    fn reconnection_delays_double_and_stay_bounded() {
        let first = Duration::from_millis(500);
        let delays: Vec<u128> = (1..=7)
            .map(|n| reconnect_delay(first, n).as_millis())
            .collect();
        assert_eq!(delays, [500, 1000, 2000, 4000, 8000, 8000, 8000]);
        assert_eq!(reconnect_delay(first, u32::MAX), Duration::from_secs(8));
    }

    fn phrase(status: &str, confidence: f32, language: Option<&str>) -> Answer {
        Answer::Phrase {
            status: status.to_owned(),
            text: "Bonjour".to_owned(),
            confidence: Some(confidence),
            language: language.map(str::to_owned),
        }
    }

    #[test]
    fn hypotheses_and_phrases_share_an_utterance_id() -> anyhow::Result<()> {
        let mut utterances = Utterances::new("S");
        let list = locales(&["fr-FR"]);
        let partial = parse_answer("speech.hypothesis", r#"{"Text":" Bonj","Offset":1}"#)?;
        let event = to_event(partial, &list, 0.5, &mut utterances)
            .ok_or_else(|| anyhow::anyhow!("none"))?;
        assert_eq!(event.kind, CaptionKind::Partial);
        assert_eq!(event.text, "Bonj");
        assert_eq!(event.utterance_id, "S-1");
        assert_eq!(event.language.as_deref(), Some("fr-FR"));
        let body = r#"{"RecognitionStatus":"Success","PrimaryLanguage":{"Language":"fr-FR","Confidence":"High"},"NBest":[{"Confidence":0.91,"Lexical":"bonjour","ITN":"bonjour","MaskedITN":"bonjour","Display":"Bonjour."}]}"#;
        let fin = to_event(
            parse_answer("speech.phrase", body)?,
            &list,
            0.5,
            &mut utterances,
        )
        .ok_or_else(|| anyhow::anyhow!("none"))?;
        assert_eq!(fin.kind, CaptionKind::Final);
        assert_eq!(fin.text, "Bonjour.");
        assert_eq!(fin.utterance_id, "S-1");
        assert_eq!(fin.confidence, Some(0.91));
        assert!(!fin.suspect);
        assert_eq!(utterances.current(), "S-2");
        Ok(())
    }

    #[test]
    fn unrecognized_unconfident_or_undeclared_finals_are_suspect() {
        let mut u = Utterances::new("S");
        let two = locales(&["fr-FR", "en-US"]);
        let suspect = |answer, list: &[String], u: &mut Utterances| {
            to_event(answer, list, 0.5, u).map(|e| e.suspect)
        };
        assert_eq!(
            suspect(phrase("Success", 0.9, Some("fr-FR")), &two, &mut u),
            Some(false)
        );
        assert_eq!(
            suspect(phrase("Success", 0.2, Some("fr-FR")), &two, &mut u),
            Some(true)
        );
        assert_eq!(
            suspect(phrase("Success", 0.9, Some("mg-MG")), &two, &mut u),
            Some(true)
        );
        assert_eq!(
            suspect(phrase("Success", 0.9, None), &two, &mut u),
            Some(true)
        );
        assert_eq!(
            suspect(phrase("BabbleTimeout", 0.9, Some("fr-FR")), &two, &mut u),
            Some(true)
        );
        let one = locales(&["fr-FR"]);
        assert_eq!(
            suspect(phrase("Success", 0.9, None), &one, &mut u),
            Some(false)
        );
    }

    #[test]
    fn with_one_locale_only_the_status_and_the_confidence_guard() {
        // Azure transcribes everything as the one declared locale and names no other language:
        // another language spoken (Malagasy for a `fr-FR` speaker) can only be caught by its
        // confidence.
        let mut u = Utterances::new("S");
        let one = locales(&["fr-FR"]);
        let suspect =
            |answer, u: &mut Utterances| to_event(answer, &one, 0.5, u).map(|e| e.suspect);
        assert_eq!(suspect(phrase("Success", 0.6, None), &mut u), Some(false));
        assert_eq!(suspect(phrase("Success", 0.4, None), &mut u), Some(true));
        assert_eq!(
            suspect(phrase("Success", 0.9, Some("fr-FR")), &mut u),
            Some(false)
        );
    }

    #[test]
    fn a_final_without_a_confidence_is_suspect() -> anyhow::Result<()> {
        let mut u = Utterances::new("S");
        let one = locales(&["fr-FR"]);
        let body = r#"{"RecognitionStatus":"Success","DisplayText":"Bonjour."}"#;
        let fin = to_event(parse_answer("speech.phrase", body)?, &one, 0.5, &mut u)
            .ok_or_else(|| anyhow::anyhow!("none"))?;
        assert_eq!(fin.confidence, None);
        assert!(fin.suspect);
        Ok(())
    }

    #[test]
    fn answers_without_text_make_no_event() -> anyhow::Result<()> {
        let mut u = Utterances::new("S");
        let list = locales(&["fr-FR"]);
        let nomatch = parse_answer("speech.phrase", r#"{"RecognitionStatus":"NoMatch"}"#)?;
        assert!(to_event(nomatch, &list, 0.5, &mut u).is_none());
        assert_eq!(parse_answer("turn.start", "{}")?, Answer::Other);
        Ok(())
    }

    #[test]
    fn parse_errors_never_carry_the_text() {
        let err = parse_answer("speech.phrase", r#"{"NBest":"private words"#)
            .err()
            .map(|e| format!("{e:#}"))
            .unwrap_or_default();
        assert!(err.contains("malformed"), "{err}");
        assert!(!err.contains("private"), "{err}");
    }
}
