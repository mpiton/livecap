//! Canary tests against the real Azure services, ignored by default.
//!
//! The Speech SDK WebSocket protocol has no public contract (see `src/speech.rs`): these tests
//! tell whether Microsoft still speaks it. Run them before each tag and during acceptance, never
//! on CI:
//!
//! ```bash
//! AZURE_SPEECH_KEY=... AZURE_TRANSLATOR_KEY=... AZURE_TRANSLATOR_REGION=francecentral \
//!   cargo test --test canary -- --ignored
//! ```
//!
//! `AZURE_SPEECH_REGION` defaults to `francecentral`; `AZURE_TRANSLATOR_URL` to the global
//! endpoint. A missing key fails the test: an ignored test is only run on purpose.

use std::time::Duration;

use livecap::config::{DEFAULT_SPEECH_REGION, speech_url};
use livecap::speech::{CaptionEvent, CaptionKind, SpeechClient};
use livecap::translator::{self, TranslatorClient};
use tokio::sync::mpsc;

/// "Bonjour, je m'appelle Claire et j'habite à Paris.", 16 kHz mono 16-bit (see
/// `tests/fixtures/README.md`).
const SENTENCE: &[u8] = include_bytes!("fixtures/canary-fr.wav");

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn install_crypto() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// The PCM samples of a WAV file: what follows its `data` chunk header.
fn pcm(wav: &[u8]) -> &[u8] {
    let at = wav
        .windows(4)
        .position(|w| w == b"data")
        .expect("no data chunk");
    &wav[at + 8..]
}

#[tokio::test]
#[ignore = "calls the real Azure AI Speech: needs AZURE_SPEECH_KEY"]
async fn azure_speech_transcribes_a_recorded_french_sentence() {
    install_crypto();
    let key = env("AZURE_SPEECH_KEY").expect("AZURE_SPEECH_KEY is required");
    let region = env("AZURE_SPEECH_REGION").unwrap_or_else(|| DEFAULT_SPEECH_REGION.to_owned());
    let client = SpeechClient::new(&speech_url(&region, None), &key, 0.5);
    let locales = vec!["fr-FR".to_owned(), "en-US".to_owned()];

    let (audio, audio_rx) = mpsc::channel(1024);
    let (events_tx, mut events) = mpsc::channel(1024);
    let task = tokio::spawn(async move {
        client
            .transcribe("canary", &locales, audio_rx, events_tx)
            .await
    });
    // Real time, then two seconds of silence so Azure ends the phrase.
    let silence = vec![0u8; 2 * 16_000 * 2];
    for chunk in pcm(SENTENCE).chunks(1280).chain(silence.chunks(1280)) {
        audio.send(chunk.to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    drop(audio);
    tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("the transcription did not end")
        .unwrap()
        .expect("Azure Speech failed");

    let mut received: Vec<CaptionEvent> = Vec::new();
    while let Ok(event) = events.try_recv() {
        received.push(event);
    }
    assert!(
        received.iter().any(|e| e.kind == CaptionKind::Partial),
        "no partial: {received:#?}"
    );
    let finals: Vec<&CaptionEvent> = received
        .iter()
        .filter(|e| e.kind == CaptionKind::Final)
        .collect();
    let paris = finals
        .iter()
        .find(|e| e.text.to_lowercase().contains("paris"))
        .unwrap_or_else(|| panic!("no final with the sentence: {received:#?}"));
    assert_eq!(paris.language.as_deref(), Some("fr-FR"), "{paris:#?}");
    assert!(paris.confidence.is_some(), "no confidence: {paris:#?}");
    assert!(!paris.suspect, "{paris:#?}");
}

#[tokio::test]
#[ignore = "calls the real Azure Translator: needs AZURE_TRANSLATOR_KEY"]
async fn azure_translator_translates_a_final() {
    install_crypto();
    let key = env("AZURE_TRANSLATOR_KEY").expect("AZURE_TRANSLATOR_KEY is required");
    let url = env("AZURE_TRANSLATOR_URL").unwrap_or_else(|| translator::DEFAULT_URL.to_owned());
    let region = env("AZURE_TRANSLATOR_REGION");
    let client = TranslatorClient::new(&url, &key, region.as_deref());
    let text = client
        .translate("Bonjour, j'habite à Paris.", "fr", "en")
        .await
        .expect("Azure Translator failed");
    assert!(text.to_lowercase().contains("paris"), "{text}");
    assert!(text.to_lowercase().contains("live"), "{text}");
}
