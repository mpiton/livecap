//! A fake Azure Translator: `POST /translate?api-version=3.0`.
//!
//! It records every request (query, key and region headers, body) and answers with a translation
//! written in advance, or with an error on demand. What it received is in `requests()` and on
//! `GET /calls`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};

use axum::Router;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

/// Translation sent for every request. Unique words, so a test can look for them in logs.
pub const TRANSLATION: &str = "Le quokka onze soixante et onze rentre";

/// One `POST /translate` as received.
#[derive(Clone, Debug, Serialize)]
pub struct Request {
    pub query: Option<String>,
    pub key: Option<String>,
    pub region: Option<String>,
    pub content_type: Option<String>,
    pub body: Value,
}

#[derive(Default)]
struct Inner {
    requests: Vec<Request>,
    failing: bool,
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

#[derive(Clone)]
pub struct FakeTranslator {
    shared: Shared,
    addr: SocketAddr,
}

impl FakeTranslator {
    pub async fn start() -> Self {
        let shared = Shared::default();
        let app = Router::new()
            .route("/translate", post(translate))
            .route("/calls", get(replay))
            .with_state(shared.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { shared, addr }
    }

    /// Base URL to pass as `AZURE_TRANSLATOR_URL`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests(&self) -> Vec<Request> {
        self.shared.lock().requests.clone()
    }

    /// Makes every request fail with a 500 from now on (or succeed again).
    pub fn set_failing(&self, failing: bool) {
        self.shared.lock().failing = failing;
    }
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

async fn translate(
    State(shared): State<Shared>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let mut inner = shared.lock();
    inner.requests.push(Request {
        query: query.clone(),
        key: header(&headers, "ocp-apim-subscription-key"),
        region: header(&headers, "ocp-apim-subscription-region"),
        content_type: header(&headers, "content-type"),
        body: serde_json::from_str(&body).unwrap_or(Value::String(body)),
    });
    let query = query.unwrap_or_default();
    let params: Vec<(&str, &str)> = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .collect();
    if !params.contains(&("api-version", "3.0")) {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({ "error": { "code": 400021, "message": "api-version" } })),
        );
    }
    if inner.failing {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({ "error": { "code": 500000, "message": "fake failure" } })),
        );
    }
    let to = params
        .iter()
        .find(|(k, _)| *k == "to")
        .map_or("", |(_, v)| *v);
    (
        StatusCode::OK,
        axum::Json(json!([{ "translations": [{ "text": TRANSLATION, "to": to }] }])),
    )
}

async fn replay(State(shared): State<Shared>) -> axum::Json<Vec<Request>> {
    axum::Json(shared.lock().requests.clone())
}
