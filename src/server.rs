//! HTTP entry points: LiveKit webhooks and a simple authenticated `join` call.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use livekit_api::access_token::TokenVerifier;
use livekit_api::webhooks::WebhookReceiver;
use tracing::{info, warn};

use crate::room::Agent;

#[derive(Clone)]
struct AppState {
    agent: Arc<Agent>,
    webhooks: WebhookReceiver,
    join_secret: Option<String>,
}

pub fn router(
    agent: Arc<Agent>,
    api_key: &str,
    api_secret: &str,
    join_secret: Option<String>,
) -> Router {
    let state = AppState {
        agent,
        webhooks: WebhookReceiver::new(TokenVerifier::with_api_key(api_key, api_secret)),
        join_secret,
    };
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/rooms/{room}/join", post(join))
        .route("/webhook", post(webhook))
        .with_state(state)
}

/// LiveKit room names are free text; keep them to something safe to log and pass along.
pub fn valid_room_name(room: &str) -> bool {
    !room.is_empty()
        && room.len() <= 128
        && room
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
}

async fn join(
    State(state): State<AppState>,
    Path(room): Path<String>,
    headers: HeaderMap,
) -> StatusCode {
    let Some(secret) = state.join_secret.as_deref() else {
        return StatusCode::NOT_FOUND;
    };
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|token| constant_time_eq(token.as_bytes(), secret.as_bytes()));
    if !authorized {
        return StatusCode::UNAUTHORIZED;
    }
    if !valid_room_name(&room) {
        return StatusCode::BAD_REQUEST;
    }
    state.agent.join(room).await;
    StatusCode::ACCEPTED
}

async fn webhook(State(state): State<AppState>, headers: HeaderMap, body: String) -> StatusCode {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let event = match state.webhooks.receive(&body, auth) {
        Ok(event) => event,
        Err(err) => {
            warn!(%err, "rejected webhook");
            return StatusCode::UNAUTHORIZED;
        }
    };
    let from_agent = event
        .participant
        .as_ref()
        .is_some_and(|p| p.identity == state.agent.identity());
    if event.event == "participant_joined"
        && !from_agent
        && let Some(room) = event.room.filter(|r| valid_room_name(&r.name))
    {
        info!(room = %room.name, "webhook: participant joined");
        state.agent.join(room.name).await;
    }
    StatusCode::OK
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::{constant_time_eq, valid_room_name};

    #[test]
    fn room_names() {
        assert!(valid_room_name("tamatave-1"));
        assert!(!valid_room_name(""));
        assert!(!valid_room_name("a/b"));
    }

    #[test]
    fn secret_comparison() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
