//! A fake Gladia live API: `POST /v2/live`, the session WebSocket and `DELETE /v2/live/{id}`.
//!
//! It answers every session with captions written in advance and records what it received, both
//! in memory (`calls()`) and on `GET /calls`, so a test can check what the agent sent.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::Router;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

/// Transcript sent by the fake for every utterance. Unique, so a test can look for it in logs.
pub const TRANSCRIPT: &str = "Quokka eleven seventy one walks home";
/// Translation sent by the fake for every utterance.
pub const TRANSLATION: &str = "Le quokka onze soixante et onze rentre";

/// How the WebSocket of each session behaves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Behavior {
    /// Sends a partial, a final and a translation once audio arrives, again every 25 chunks.
    #[default]
    Captions,
    /// Closes the WebSocket with an error code as soon as audio arrives.
    CloseWithError,
}

/// One `POST /v2/live` as received.
#[derive(Clone, Debug, Serialize)]
pub struct Open {
    pub query: Option<String>,
    pub api_key: Option<String>,
    pub body: Value,
}

/// Everything the fake received.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Calls {
    pub opens: Vec<Open>,
    /// Session ids, in the order the WebSockets were opened.
    pub connected: Vec<String>,
    /// Audio bytes received per session id.
    pub audio: Vec<(String, usize)>,
    /// Every `DELETE /v2/live/{id}`, including the refused ones.
    pub delete_attempts: Vec<String>,
    /// Session ids whose deletion succeeded.
    pub deleted: Vec<String>,
}

#[derive(Default)]
struct Inner {
    calls: Calls,
    behavior: Behavior,
    /// Number of upcoming `DELETE` calls to refuse with 409, as Gladia does before a session is traced.
    refuse_deletes: usize,
    ws_port: u16,
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
pub struct FakeGladia {
    shared: Shared,
    http_addr: SocketAddr,
}

impl FakeGladia {
    pub async fn start() -> Self {
        let shared = Shared::default();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        shared.lock().ws_port = ws.local_addr().unwrap().port();
        tokio::spawn(accept_websockets(ws, shared.clone()));

        let app = Router::new()
            .route("/v2/live", post(open))
            .route("/v2/live/{id}", delete(remove))
            .route("/calls", get(replay))
            .with_state(shared.clone());
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(http, app).await.unwrap() });
        Self { shared, http_addr }
    }

    /// Base URL to pass as `GLADIA_API_URL`.
    pub fn url(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    pub fn calls(&self) -> Calls {
        self.shared.lock().calls.clone()
    }

    pub fn set_behavior(&self, behavior: Behavior) {
        self.shared.lock().behavior = behavior;
    }

    pub fn refuse_next_deletes(&self, count: usize) {
        self.shared.lock().refuse_deletes = count;
    }
}

async fn open(
    State(shared): State<Shared>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let mut inner = shared.lock();
    let id = format!("session-{}", inner.calls.opens.len() + 1);
    inner.calls.opens.push(Open {
        query,
        api_key: headers
            .get("x-gladia-key")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned),
        body: serde_json::from_str(&body).unwrap_or(Value::String(body)),
    });
    let url = format!("ws://127.0.0.1:{}/v2/live/{id}", inner.ws_port);
    (
        StatusCode::CREATED,
        axum::Json(json!({ "id": id, "url": url })),
    )
}

async fn remove(State(shared): State<Shared>, Path(id): Path<String>) -> StatusCode {
    let mut inner = shared.lock();
    inner.calls.delete_attempts.push(id.clone());
    if inner.refuse_deletes > 0 {
        inner.refuse_deletes -= 1;
        return StatusCode::CONFLICT;
    }
    inner.calls.deleted.push(id);
    StatusCode::ACCEPTED
}

async fn replay(State(shared): State<Shared>) -> axum::Json<Calls> {
    axum::Json(shared.lock().calls.clone())
}

async fn accept_websockets(listener: TcpListener, shared: Shared) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(session(stream, shared.clone()));
    }
}

async fn session(stream: tokio::net::TcpStream, shared: Shared) {
    let mut path = String::new();
    let callback = |request: &Request, response: Response| {
        path = request.uri().path().to_owned();
        Ok(response)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let id = path.rsplit('/').next().unwrap_or_default().to_owned();
    let behavior = {
        let mut inner = shared.lock();
        inner.calls.connected.push(id.clone());
        inner.calls.audio.push((id.clone(), 0));
        inner.behavior
    };
    let (mut sink, mut stream) = ws.split();
    let mut chunks = 0usize;
    let mut utterance = 0usize;
    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Binary(bytes) => {
                {
                    let mut inner = shared.lock();
                    if let Some(entry) = inner.calls.audio.iter_mut().find(|(s, _)| *s == id) {
                        entry.1 += bytes.len();
                    }
                }
                if behavior == Behavior::CloseWithError {
                    let frame = tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: 4500.into(),
                        reason: "fake failure".into(),
                    };
                    let _ = sink.send(Message::Close(Some(frame))).await;
                    return;
                }
                if chunks % 25 == 0 {
                    utterance += 1;
                    for text in script(&format!("{id}-u{utterance}")) {
                        if sink.send(Message::text(text)).await.is_err() {
                            return;
                        }
                    }
                }
                chunks += 1;
            }
            Message::Text(text) if text.contains("stop_recording") => {
                let _ = sink.send(Message::Close(None)).await;
                return;
            }
            Message::Close(_) => return,
            _ => {}
        }
    }
}

fn script(utterance_id: &str) -> Vec<String> {
    vec![
        json!({ "type": "transcript", "data": { "id": utterance_id, "is_final": false,
            "utterance": { "text": TRANSCRIPT, "language": "en" } } })
        .to_string(),
        json!({ "type": "transcript", "data": { "id": utterance_id, "is_final": true,
            "utterance": { "text": TRANSCRIPT, "language": "en", "confidence": 0.93 } } })
        .to_string(),
        json!({ "type": "translation", "data": { "utterance_id": utterance_id,
            "utterance": { "text": TRANSCRIPT, "language": "en" }, "original_language": "en",
            "target_language": "fr", "translated_utterance": { "text": TRANSLATION } } })
        .to_string(),
    ]
}
