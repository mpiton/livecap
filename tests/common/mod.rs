//! Test bench shared by the integration tests: a fake Azure AI Speech, a fake Azure Translator
//! and a local LiveKit server.

#![allow(
    dead_code,
    reason = "each test binary uses a different part of the bench"
)]

pub mod fake_speech;
pub mod fake_translator;
pub mod livekit;

use fake_speech::FakeSpeech;
use fake_translator::FakeTranslator;

/// Keys the agent is started with. Unique, so a test can look for them in logs.
pub const SPEECH_KEY: &str = "speech-key-5d81c2e7a9";
pub const TRANSLATOR_KEY: &str = "translator-key-b04f6a13e2";
/// Region of the Translator resource the agent is started with.
pub const TRANSLATOR_REGION: &str = "francecentral";

/// Both Azure fakes.
#[derive(Clone)]
pub struct Azure {
    pub speech: FakeSpeech,
    pub translator: FakeTranslator,
}

impl Azure {
    pub async fn start() -> Self {
        Self {
            speech: FakeSpeech::start().await,
            translator: FakeTranslator::start().await,
        }
    }
}

/// Serializes the tests that run WebRTC: one LiveKit server and one agent at a time keeps them
/// reliable on a small CI runner.
pub static LIVEKIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
