//! Gladia real-time speech-to-text with translation (`POST /v2/live`, then a WebSocket per speaker).

use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::audio::SAMPLE_RATE;

const MAX_RECONNECTS: u32 = 5;
/// How long to wait for the last results after `stop_recording`.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct GladiaClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
    speech_threshold: f32,
}

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub url: String,
    pub translate: bool,
}

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
    /// Language detected by Gladia for this utterance.
    pub language: Option<String>,
    pub confidence: Option<f32>,
}

impl GladiaClient {
    pub fn new(base_url: &str, api_key: &str, speech_threshold: f32) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_owned(),
            api_key: api_key.to_owned(),
            speech_threshold,
        }
    }

    /// Opens a live session for one speaker. `target` = language the listener reads; `None` or equal to `lang` = no translation.
    pub async fn open(&self, lang: &str, target: Option<&str>) -> Result<Session> {
        let target = target.filter(|t| *t != lang);
        let body = session_body(lang, target, self.speech_threshold);
        let response = self
            .http
            .post(format!("{}/v2/live", self.base_url))
            .header("x-gladia-key", &self.api_key)
            .json(&body)
            .send()
            .await
            .context("Gladia unreachable")?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "Gladia refused the session ({status}): {}",
                text.chars().take(300).collect::<String>()
            );
        }
        #[derive(Deserialize)]
        struct Created {
            id: String,
            url: String,
        }
        let created: Created = serde_json::from_str(&text).context("unexpected Gladia response")?;
        Ok(Session {
            id: created.id,
            url: created.url,
            translate: target.is_some(),
        })
    }
}

fn session_body(lang: &str, target: Option<&str>, speech_threshold: f32) -> serde_json::Value {
    let mut body = json!({
        "encoding": "wav/pcm",
        "sample_rate": SAMPLE_RATE,
        "bit_depth": 16,
        "channels": 1,
        "language_config": { "languages": [lang], "code_switching": false },
        "pre_processing": { "speech_threshold": speech_threshold },
        "messages_config": {
            "receive_partial_transcripts": true,
            "receive_final_transcripts": true,
            "receive_realtime_processing_events": target.is_some()
        }
    });
    if let (Some(target), Some(obj)) = (target, body.as_object_mut()) {
        obj.insert(
            "realtime_processing".to_owned(),
            json!({
                "translation": true,
                "translation_config": { "target_languages": [target], "context_adaptation": true, "informal": true }
            }),
        );
    }
    body
}

/// Streams audio chunks to the session and forwards captions until `audio` closes.
/// Reconnects to the same session URL a few times if the WebSocket drops.
pub async fn run_session(
    session: Session,
    mut audio: mpsc::Receiver<Vec<u8>>,
    events: mpsc::Sender<CaptionEvent>,
) -> Result<()> {
    let mut attempts = 0;
    loop {
        let (ws, _) = match connect_async(session.url.as_str()).await {
            Ok(ok) => ok,
            Err(err) if attempts < MAX_RECONNECTS => {
                attempts += 1;
                warn!(session = %session.id, %err, attempts, "Gladia connection failed, retrying");
                tokio::time::sleep(Duration::from_secs(u64::from(attempts))).await;
                continue;
            }
            Err(err) => return Err(err).context("Gladia WebSocket unreachable"),
        };
        info!(session = %session.id, "Gladia connected");
        let (mut sink, mut stream) = ws.split();
        let mut stopping = false;
        loop {
            tokio::select! {
                chunk = audio.recv(), if !stopping => match chunk {
                    Some(bytes) => {
                        if let Err(err) = sink.send(Message::binary(bytes)).await {
                            warn!(session = %session.id, %err, "audio send failed");
                            break;
                        }
                    }
                    None => {
                        stopping = true;
                        let _ = sink.send(Message::text(r#"{"type":"stop_recording"}"#)).await;
                    }
                },
                message = async {
                    if stopping {
                        tokio::time::timeout(DRAIN_TIMEOUT, stream.next()).await.unwrap_or(None)
                    } else {
                        stream.next().await
                    }
                } => match message {
                    Some(Ok(Message::Text(text))) => forward(text.as_str(), &events).await,
                    Some(Ok(Message::Close(_))) | None if stopping => return Ok(()),
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) if stopping => return Ok(()),
                    Some(Err(err)) => {
                        warn!(session = %session.id, %err, "Gladia stream error");
                        break;
                    }
                },
            }
        }
        if attempts >= MAX_RECONNECTS {
            bail!(
                "Gladia session {} closed {MAX_RECONNECTS} times, giving up",
                session.id
            );
        }
        attempts += 1;
        tokio::time::sleep(Duration::from_secs(u64::from(attempts))).await;
    }
}

async fn forward(text: &str, events: &mpsc::Sender<CaptionEvent>) {
    match parse_message(text) {
        Ok(Some(event)) => {
            let _ = events.send(event).await;
        }
        Ok(None) => {}
        Err(err) => debug!(%err, "ignored Gladia message"),
    }
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    data: Option<Data>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct Data {
    id: Option<String>,
    utterance_id: Option<String>,
    is_final: Option<bool>,
    utterance: Option<Utterance>,
    original_language: Option<String>,
    translated_utterance: Option<Translated>,
}

#[derive(Deserialize)]
struct Utterance {
    text: String,
    language: Option<String>,
    confidence: Option<f32>,
}

#[derive(Deserialize)]
struct Translated {
    text: String,
}

/// Maps one Gladia WebSocket message to a caption event (`Ok(None)` for messages we don't use).
pub fn parse_message(raw: &str) -> Result<Option<CaptionEvent>> {
    let envelope: Envelope = serde_json::from_str(raw)?;
    if envelope.kind == "error" || envelope.error.is_some() {
        bail!(
            "Gladia error: {}",
            raw.chars().take(300).collect::<String>()
        );
    }
    let Some(data) = envelope.data else {
        return Ok(None);
    };
    match envelope.kind.as_str() {
        "transcript" => {
            let (Some(id), Some(utterance)) = (data.id, data.utterance) else {
                return Ok(None);
            };
            let text = utterance.text.trim().to_owned();
            if text.is_empty() {
                return Ok(None);
            }
            Ok(Some(CaptionEvent {
                utterance_id: id,
                kind: if data.is_final.unwrap_or(false) {
                    CaptionKind::Final
                } else {
                    CaptionKind::Partial
                },
                text,
                original: None,
                language: utterance.language,
                confidence: utterance.confidence,
            }))
        }
        "translation" => {
            let Some(translated) = data.translated_utterance else {
                return Ok(None);
            };
            let id = data.utterance_id.or(data.id).unwrap_or_default();
            Ok(Some(CaptionEvent {
                utterance_id: id,
                kind: CaptionKind::Translation,
                text: translated.text.trim().to_owned(),
                original: data.utterance.as_ref().map(|u| u.text.trim().to_owned()),
                language: data
                    .utterance
                    .as_ref()
                    .and_then(|u| u.language.clone())
                    .or(data.original_language),
                confidence: data.utterance.and_then(|u| u.confidence),
            }))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::{CaptionKind, parse_message, session_body};

    #[test]
    fn parses_partial_and_final_transcripts() -> anyhow::Result<()> {
        let partial = r#"{"type":"transcript","data":{"id":"u1","is_final":false,"utterance":{"text":" Bonj","language":"fr"}}}"#;
        let event = parse_message(partial)?.ok_or_else(|| anyhow::anyhow!("no event"))?;
        assert_eq!(event.kind, CaptionKind::Partial);
        assert_eq!(event.text, "Bonj");

        let fin = r#"{"type":"transcript","data":{"id":"u1","is_final":true,"utterance":{"text":"Bonjour","language":"fr","confidence":0.9}}}"#;
        let event = parse_message(fin)?.ok_or_else(|| anyhow::anyhow!("no event"))?;
        assert_eq!(event.kind, CaptionKind::Final);
        assert_eq!(event.confidence, Some(0.9));
        Ok(())
    }

    #[test]
    fn parses_translation_with_original() -> anyhow::Result<()> {
        let raw = r#"{"type":"translation","data":{"utterance_id":"u1","utterance":{"text":"Bonjour","language":"fr"},"original_language":"fr","target_language":"mg","translated_utterance":{"text":"Manao ahoana"}}}"#;
        let event = parse_message(raw)?.ok_or_else(|| anyhow::anyhow!("no event"))?;
        assert_eq!(event.kind, CaptionKind::Translation);
        assert_eq!(event.utterance_id, "u1");
        assert_eq!(event.original.as_deref(), Some("Bonjour"));
        assert_eq!(event.language.as_deref(), Some("fr"));
        Ok(())
    }

    #[test]
    fn ignores_other_messages_and_reports_errors() -> anyhow::Result<()> {
        assert!(parse_message(r#"{"type":"speech_start","data":{}}"#)?.is_none());
        assert!(parse_message(r#"{"type":"error","error":{"message":"boom"}}"#).is_err());
        Ok(())
    }

    #[test]
    fn translation_only_when_languages_differ() {
        let with = session_body("mg", Some("fr"), 0.75);
        assert!(with.get("realtime_processing").is_some());
        let without = session_body("fr", None, 0.75);
        assert!(without.get("realtime_processing").is_none());
    }
}
