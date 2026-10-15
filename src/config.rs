//! Configuration read from environment variables.

use std::env;

use anyhow::{Context, Result};

/// Participant attribute holding the language a participant speaks (ISO 639-1, e.g. `fr`).
pub const DEFAULT_LANG_ATTRIBUTE: &str = "livecap.lang";
/// Participant attribute holding the language this participant wants to read captions in.
pub const DEFAULT_TARGET_ATTRIBUTE: &str = "livecap.target";

#[derive(Clone, Debug)]
pub struct Config {
    pub livekit_url: String,
    pub livekit_api_key: String,
    pub livekit_api_secret: String,
    pub gladia_api_key: String,
    pub gladia_api_url: String,
    /// Shared secret for `POST /rooms/{room}/join`. When absent, only signed LiveKit webhooks can start the agent.
    pub join_secret: Option<String>,
    pub listen_addr: String,
    pub agent_identity: String,
    pub lang_attribute: String,
    pub target_attribute: String,
    /// Gladia speech threshold (0..1). Higher = fewer captions invented from silence.
    pub speech_threshold: f32,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            livekit_url: required("LIVEKIT_URL")?,
            livekit_api_key: required("LIVEKIT_API_KEY")?,
            livekit_api_secret: required("LIVEKIT_API_SECRET")?,
            gladia_api_key: required("GLADIA_API_KEY")?,
            gladia_api_url: optional("GLADIA_API_URL")
                .unwrap_or_else(|| "https://api.gladia.io".to_owned()),
            join_secret: optional("LIVECAP_JOIN_SECRET"),
            listen_addr: optional("LIVECAP_LISTEN_ADDR")
                .unwrap_or_else(|| "0.0.0.0:8790".to_owned()),
            agent_identity: optional("LIVECAP_AGENT_IDENTITY")
                .unwrap_or_else(|| "livecap".to_owned()),
            lang_attribute: optional("LIVECAP_LANG_ATTRIBUTE")
                .unwrap_or_else(|| DEFAULT_LANG_ATTRIBUTE.to_owned()),
            target_attribute: optional("LIVECAP_TARGET_ATTRIBUTE")
                .unwrap_or_else(|| DEFAULT_TARGET_ATTRIBUTE.to_owned()),
            speech_threshold: match optional("GLADIA_SPEECH_THRESHOLD") {
                Some(v) => v
                    .parse()
                    .context("GLADIA_SPEECH_THRESHOLD must be a number between 0 and 1")?,
                None => 0.75,
            },
        })
    }
}

fn required(name: &str) -> Result<String> {
    optional(name).with_context(|| format!("missing environment variable {name}"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}
