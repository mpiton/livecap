//! Configuration read from environment variables.

use std::env;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::speech::{self, DEFAULT_MIN_CONFIDENCE};
use crate::translator;

/// Participant attribute holding the locales a participant speaks: one Azure locale or a
/// comma-separated list of at most 10 (e.g. `fr-FR` or `fr-FR,en-US`).
pub const DEFAULT_LANG_ATTRIBUTE: &str = "livecap.lang";
/// Participant attribute holding the language this participant wants to read captions in.
pub const DEFAULT_TARGET_ATTRIBUTE: &str = "livecap.target";
/// Participant attribute switching the transcription of a participant's voice: `on` or absent,
/// transcribed; any other value, `off` included, not transcribed and no connection to Azure.
pub const DEFAULT_TRANSCRIBE_ATTRIBUTE: &str = "livecap.transcribe";

/// Default limit on the calls still running after SIGTERM: two hours, the longest call expected.
/// The service manager's stop timeout must be longer (see `docs/deploy.md`).
pub const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2 * 3600);

/// Azure region of the speech service when `AZURE_SPEECH_REGION` is not set.
pub const DEFAULT_SPEECH_REGION: &str = "francecentral";

#[derive(Clone, Debug)]
pub struct Config {
    pub livekit_url: String,
    pub livekit_api_key: String,
    pub livekit_api_secret: String,
    pub azure_speech_key: String,
    /// Azure region that transcribes the audio, e.g. `francecentral`.
    pub azure_speech_region: String,
    /// Base WebSocket URL of Azure AI Speech: `AZURE_SPEECH_URL`, or the region's.
    pub azure_speech_url: String,
    /// Finals with a lower confidence are suspect and never reach the listeners.
    pub azure_speech_min_confidence: f32,
    pub azure_translator_key: String,
    /// Region of the Translator resource, sent as `Ocp-Apim-Subscription-Region` when set.
    pub azure_translator_region: Option<String>,
    pub azure_translator_url: String,
    /// Shared secret for `POST /rooms/{room}/join`. When absent, only signed LiveKit webhooks can start the agent.
    pub join_secret: Option<String>,
    pub listen_addr: String,
    pub agent_identity: String,
    pub lang_attribute: String,
    pub target_attribute: String,
    pub transcribe_attribute: String,
    /// Rooms followed at once; `join` answers `full` beyond. Each room holds one Azure Speech
    /// connection per speaker.
    pub max_rooms: usize,
    /// After SIGTERM, how long the current calls may go on before the agent leaves them.
    pub drain_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let azure_speech_region =
            optional("AZURE_SPEECH_REGION").unwrap_or_else(|| DEFAULT_SPEECH_REGION.to_owned());
        Ok(Self {
            livekit_url: required("LIVEKIT_URL")?,
            livekit_api_key: required("LIVEKIT_API_KEY")?,
            livekit_api_secret: required("LIVEKIT_API_SECRET")?,
            azure_speech_key: required("AZURE_SPEECH_KEY")?,
            azure_speech_url: speech_url(&azure_speech_region, optional("AZURE_SPEECH_URL")),
            azure_speech_region,
            azure_speech_min_confidence: match optional("AZURE_SPEECH_MIN_CONFIDENCE") {
                Some(v) => v
                    .parse()
                    .ok()
                    .filter(|c| (0.0..=1.0).contains(c))
                    .context("AZURE_SPEECH_MIN_CONFIDENCE must be a number between 0 and 1")?,
                None => DEFAULT_MIN_CONFIDENCE,
            },
            azure_translator_key: required("AZURE_TRANSLATOR_KEY")?,
            azure_translator_region: optional("AZURE_TRANSLATOR_REGION"),
            azure_translator_url: optional("AZURE_TRANSLATOR_URL")
                .unwrap_or_else(|| translator::DEFAULT_URL.to_owned()),
            join_secret: optional("LIVECAP_JOIN_SECRET"),
            listen_addr: optional("LIVECAP_LISTEN_ADDR")
                .unwrap_or_else(|| "0.0.0.0:8790".to_owned()),
            agent_identity: optional("LIVECAP_AGENT_IDENTITY")
                .unwrap_or_else(|| "livecap".to_owned()),
            lang_attribute: optional("LIVECAP_LANG_ATTRIBUTE")
                .unwrap_or_else(|| DEFAULT_LANG_ATTRIBUTE.to_owned()),
            target_attribute: optional("LIVECAP_TARGET_ATTRIBUTE")
                .unwrap_or_else(|| DEFAULT_TARGET_ATTRIBUTE.to_owned()),
            transcribe_attribute: optional("LIVECAP_TRANSCRIBE_ATTRIBUTE")
                .unwrap_or_else(|| DEFAULT_TRANSCRIBE_ATTRIBUTE.to_owned()),
            max_rooms: match optional("LIVECAP_MAX_ROOMS") {
                Some(v) => v
                    .parse()
                    .ok()
                    .filter(|n| *n > 0)
                    .context("LIVECAP_MAX_ROOMS must be a positive integer")?,
                None => 15,
            },
            drain_timeout: match optional("LIVECAP_DRAIN_TIMEOUT_SECS") {
                Some(v) => Duration::from_secs(
                    v.parse()
                        .context("LIVECAP_DRAIN_TIMEOUT_SECS must be a number of seconds")?,
                ),
                None => DEFAULT_DRAIN_TIMEOUT,
            },
        })
    }
}

/// The speech service's base URL: the override when set (a fake, in tests), else the region's.
pub fn speech_url(region: &str, url_override: Option<String>) -> String {
    url_override.unwrap_or_else(|| speech::region_url(region))
}

fn required(name: &str) -> Result<String> {
    optional(name).with_context(|| format!("missing environment variable {name}"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SPEECH_REGION, speech_url};

    #[test]
    fn the_speech_service_runs_in_france_central_unless_told_otherwise() {
        assert_eq!(
            speech_url(DEFAULT_SPEECH_REGION, None),
            "wss://francecentral.stt.speech.microsoft.com"
        );
        assert_eq!(
            speech_url("westeurope", None),
            "wss://westeurope.stt.speech.microsoft.com"
        );
        assert_eq!(
            speech_url("westeurope", Some("ws://127.0.0.1:9".to_owned())),
            "ws://127.0.0.1:9"
        );
    }
}
