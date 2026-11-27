//! The Azure clients against the fakes: what they send and how they read the answers.

mod common;

use std::time::Duration;

use common::fake_speech::{Connection, FakeSpeech, PARTIAL, Phrase, TRANSCRIPT};
use common::fake_translator::{FakeTranslator, TRANSLATION};
use livecap::speech::{CaptionEvent, CaptionKind, SpeechClient};
use livecap::translator::TranslatorClient;
use serde_json::json;
use tokio::sync::mpsc;

/// 40 ms of PCM 16 kHz mono 16-bit.
const CHUNK: usize = 1280;

fn locales(raw: &[&str]) -> Vec<String> {
    raw.iter().map(|l| (*l).to_owned()).collect()
}

fn client(url: &str) -> SpeechClient {
    SpeechClient::new(url, "speech-key-1", 0.5)
        .with_first_reconnect_delay(Duration::from_millis(50))
}

/// A running transcription: feed it audio, then `finish` it.
struct Run {
    audio: mpsc::Sender<Vec<u8>>,
    events: mpsc::Receiver<CaptionEvent>,
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Run {
    fn start(client: SpeechClient, list: &[&str]) -> Self {
        let list = locales(list);
        let (audio, audio_rx) = mpsc::channel(512);
        let (events_tx, events) = mpsc::channel(512);
        let task =
            tokio::spawn(async move { client.transcribe("S", &list, audio_rx, events_tx).await });
        Self {
            audio,
            events,
            task,
        }
    }

    async fn send(&self, chunks: usize) {
        for _ in 0..chunks {
            self.audio.send(vec![0u8; CHUNK]).await.unwrap();
        }
    }

    /// Ends the audio, waits for the transcription to end, returns every event.
    async fn finish(self) -> (anyhow::Result<()>, Vec<CaptionEvent>) {
        let Self {
            audio,
            mut events,
            task,
        } = self;
        drop(audio);
        let result = tokio::time::timeout(Duration::from_secs(20), task)
            .await
            .expect("the transcription did not end")
            .unwrap();
        let mut all = Vec::new();
        while let Ok(event) = events.try_recv() {
            all.push(event);
        }
        (result, all)
    }
}

async fn transcribe(fake: &FakeSpeech, list: &[&str], chunks: usize) -> Vec<CaptionEvent> {
    let run = Run::start(client(&fake.url()), list);
    run.send(chunks).await;
    let (result, events) = run.finish().await;
    result.unwrap();
    events
}

fn only_connection(fake: &FakeSpeech) -> Connection {
    let connections = fake.connections();
    assert_eq!(connections.len(), 1, "{connections:#?}");
    connections.into_iter().next().unwrap()
}

fn is_id(value: Option<&String>) -> bool {
    value.is_some_and(|v| v.len() == 32 && v.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[tokio::test]
async fn one_locale_pins_the_url_the_headers_and_the_exact_bodies() {
    let fake = FakeSpeech::start().await;
    transcribe(&fake, &["fr-FR"], 3).await;
    let c = only_connection(&fake);

    assert_eq!(c.path, "/stt/speech/universal/v2");
    assert_eq!(c.query, "language=fr-FR&format=detailed&profanity=raw");
    assert_eq!(
        c.headers
            .get("ocp-apim-subscription-key")
            .map(String::as_str),
        Some("speech-key-1")
    );
    assert!(is_id(c.headers.get("x-connectionid")), "{:?}", c.headers);

    let config = c.config.expect("no speech.config");
    assert_eq!(
        config.body,
        json!({
            "context": {
                "system": { "name": "SpeechSDK", "version": "1.53.0", "build": "livecap", "lang": "Rust" },
                "os": { "platform": "Linux", "name": "livecap", "version": env!("CARGO_PKG_VERSION") },
                "audio": { "source": {
                    "manufacturer": "livecap", "model": "LiveKit track", "connectivity": "Unknown",
                    "type": "Stream", "samplerate": 16000, "bitspersample": 16, "channelcount": 1
                } }
            }
        })
    );
    assert_eq!(
        config.headers.get("path").map(String::as_str),
        Some("speech.config")
    );
    assert_eq!(
        config.headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    assert!(
        config
            .headers
            .get("x-timestamp")
            .is_some_and(|t| t.ends_with('Z'))
    );
    let request = config.headers.get("x-requestid");
    assert!(is_id(request));

    assert_eq!(c.contexts.len(), 1);
    let context = &c.contexts[0];
    assert_eq!(
        context.body,
        json!({
            "phraseDetection": { "mode": "Conversation", "language": "fr-FR" },
            "phraseOutput": { "format": "Detailed", "detailed": { "options": [] } }
        })
    );
    assert_eq!(context.headers.get("x-requestid"), request);

    assert_eq!(
        c.first_audio_headers.get("path").map(String::as_str),
        Some("audio")
    );
    assert_eq!(c.first_audio_headers.get("x-requestid"), request);
    assert!(
        c.riff,
        "the first audio message is not a 16 kHz mono 16-bit RIFF header"
    );
    assert_eq!(c.wave_headers, 1);
    assert_eq!((c.audio_frames, c.audio_bytes), (3, 3 * CHUNK));
    assert_eq!(c.malformed, 0);
    assert!(
        c.end_of_audio,
        "the empty audio message did not end the stream"
    );
    common::livekit::wait_until(Duration::from_secs(5), || async {
        fake.connections().first().is_some_and(|c| c.closed)
    })
    .await;
}

#[tokio::test]
async fn several_locales_turn_on_continuous_language_detection_among_them_only() {
    let fake = FakeSpeech::start().await;
    transcribe(&fake, &["fr-FR", "en-US"], 1).await;
    let c = only_connection(&fake);
    assert_eq!(c.query, "format=detailed&lidEnabled=true&profanity=raw");
    assert_eq!(
        c.contexts[0].body,
        json!({
            "phraseDetection": { "mode": "Conversation" },
            "languageId": {
                "languages": ["fr-FR", "en-US"],
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
    );
}

#[tokio::test]
async fn azure_never_stores_audio_and_never_identifies_speakers() {
    let fake = FakeSpeech::start().await;
    transcribe(&fake, &["fr-FR"], 1).await;
    transcribe(&fake, &["fr-FR", "en-US"], 1).await;
    for c in fake.connections() {
        assert!(
            !c.query.to_lowercase().contains("storeaudio"),
            "{}",
            c.query
        );
        assert_eq!(
            c.path, "/stt/speech/universal/v2",
            "not a conversation transcription"
        );
        let sent = serde_json::to_string(&(c.config, c.contexts))
            .unwrap()
            .to_lowercase();
        for word in ["storeaudio", "diarization", "speakerid", "speaker"] {
            assert!(!sent.contains(word), "`{word}` sent: {sent}");
        }
    }
}

#[tokio::test]
async fn partials_and_finals_share_an_utterance_id() {
    let fake = FakeSpeech::start().await;
    let events = transcribe(&fake, &["en-US"], 1).await;
    assert_eq!(events.len(), 2, "{events:#?}");
    let (partial, fin) = (&events[0], &events[1]);
    assert_eq!(partial.kind, CaptionKind::Partial);
    assert_eq!(partial.text, PARTIAL);
    assert_eq!(fin.kind, CaptionKind::Final);
    assert_eq!(fin.text, TRANSCRIPT);
    assert_eq!(fin.utterance_id, partial.utterance_id);
    assert_eq!(fin.language.as_deref(), Some("en-US"));
    assert_eq!(fin.confidence, Some(0.93));
    assert!(!fin.suspect);
}

async fn final_with(phrase: Phrase, list: &[&str]) -> CaptionEvent {
    let fake = FakeSpeech::start().await;
    fake.set_phrase(phrase);
    let events = transcribe(&fake, list, 1).await;
    events
        .into_iter()
        .find(|e| e.kind == CaptionKind::Final)
        .expect("no final")
}

#[tokio::test]
async fn a_final_below_the_minimum_confidence_is_suspect() {
    let low = Phrase {
        confidence: 0.3,
        ..Phrase::default()
    };
    assert!(final_with(low, &["en-US"]).await.suspect);
}

#[tokio::test]
async fn a_final_in_an_undeclared_language_is_suspect() {
    // A Malagasy speaker who declared French: the text Azure forces into French is not shown.
    let elsewhere = Phrase {
        language: Some("mg-MG"),
        ..Phrase::default()
    };
    assert!(final_with(elsewhere, &["fr-FR", "en-US"]).await.suspect);
}

#[tokio::test]
async fn a_final_not_recognized_successfully_is_suspect() {
    let babble = Phrase {
        status: "BabbleTimeout",
        ..Phrase::default()
    };
    assert!(final_with(babble, &["en-US"]).await.suspect);
}

#[tokio::test]
async fn a_connection_cut_mid_stream_is_reopened_and_the_transcript_goes_on() {
    let fake = FakeSpeech::start().await;
    fake.cut_next(1);
    let run = Run::start(client(&fake.url()), &["en-US"]);
    run.send(5).await;
    common::livekit::wait_until(Duration::from_secs(10), || async {
        fake.connections().len() == 2
    })
    .await;
    run.send(5).await;
    let (result, events) = run.finish().await;
    result.unwrap();

    let connections = fake.connections();
    assert!(connections[0].cut);
    let second = &connections[1];
    assert!(second.config.is_some() && second.riff && !second.contexts.is_empty());
    assert!(second.audio_frames > 0 && second.end_of_audio);
    let finals: Vec<&CaptionEvent> = events
        .iter()
        .filter(|e| e.kind == CaptionKind::Final)
        .collect();
    assert!(finals.len() >= 2, "{events:#?}");
    assert_ne!(finals[0].utterance_id, finals[1].utterance_id);
}

#[tokio::test]
async fn a_turn_ended_by_the_service_is_followed_by_a_new_turn_on_the_same_connection() {
    let fake = FakeSpeech::start().await;
    fake.end_turn_next(1);
    let run = Run::start(client(&fake.url()), &["en-US"]);
    run.send(1).await;
    common::livekit::wait_until(Duration::from_secs(10), || async {
        fake.connections()
            .first()
            .is_some_and(|c| c.wave_headers == 2)
    })
    .await;
    run.send(25).await;
    let (result, events) = run.finish().await;
    result.unwrap();
    let c = only_connection(&fake);
    assert_eq!(c.contexts.len(), 2);
    let first = c.contexts[0].headers.get("x-requestid");
    let second = c.contexts[1].headers.get("x-requestid");
    assert!(
        is_id(second) && first != second,
        "a new turn needs a new request id"
    );
    assert_eq!(c.contexts[1].body, c.contexts[0].body);
    let finals = events
        .iter()
        .filter(|e| e.kind == CaptionKind::Final)
        .count();
    assert_eq!(finals, 2, "{events:#?}");
}

#[tokio::test]
async fn an_unreachable_service_is_given_up_after_bounded_attempts() {
    let nowhere = format!("ws://127.0.0.1:{}", common::livekit::free_tcp_port());
    let client = SpeechClient::new(&nowhere, "speech-key-1", 0.5)
        .with_first_reconnect_delay(Duration::from_millis(1));
    let run = Run::start(client, &["en-US"]);
    let result = tokio::time::timeout(Duration::from_secs(20), run.task)
        .await
        .expect("the client kept retrying")
        .unwrap();
    let err = format!(
        "{:#}",
        result.expect_err("an unreachable service is not an error")
    );
    assert!(err.contains("8 times"), "{err}");
    assert!(!err.contains("speech-key-1"), "{err}");
}

#[tokio::test]
async fn the_fake_replays_what_it_received() {
    let fake = FakeSpeech::start().await;
    transcribe(&fake, &["fr-FR"], 2).await;
    let replay: serde_json::Value = reqwest::get(fake.calls_url())
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay["connections"][0]["audio_frames"], 2);
    assert_eq!(
        replay["connections"][0]["query"],
        "language=fr-FR&format=detailed&profanity=raw"
    );
}

#[tokio::test]
async fn translator_requests_name_the_languages_the_key_and_the_region() {
    let fake = FakeTranslator::start().await;
    let client = TranslatorClient::new(&fake.url(), "translator-key-1", Some("francecentral"));
    let text = client.translate("Bonjour", "fr", "en").await.unwrap();
    assert_eq!(text, TRANSLATION);
    let requests = fake.requests();
    assert_eq!(requests.len(), 1);
    let r = &requests[0];
    assert_eq!(r.query.as_deref(), Some("api-version=3.0&from=fr&to=en"));
    assert_eq!(r.key.as_deref(), Some("translator-key-1"));
    assert_eq!(r.region.as_deref(), Some("francecentral"));
    assert_eq!(r.content_type.as_deref(), Some("application/json"));
    assert_eq!(r.body, json!([{ "Text": "Bonjour" }]));

    let global = TranslatorClient::new(&fake.url(), "translator-key-1", None);
    global.translate("Bonjour", "fr", "en").await.unwrap();
    assert_eq!(fake.requests()[1].region, None);
}

#[tokio::test]
async fn translator_errors_carry_neither_the_text_nor_the_key() {
    let fake = FakeTranslator::start().await;
    fake.set_failing(true);
    let client = TranslatorClient::new(&fake.url(), "translator-key-1", None);
    let err = client
        .translate("Private words", "fr", "en")
        .await
        .map(|_| ())
        .map_err(|e| format!("{e:#}"))
        .expect_err("a failing translator is not an error");
    assert!(err.contains("500"), "{err}");
    for secret in ["Private", "translator-key-1", "fake failure"] {
        assert!(!err.contains(secret), "{err}");
    }
    let replay: serde_json::Value = reqwest::get(format!("{}/calls", fake.url()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replay[0]["body"], json!([{ "Text": "Private words" }]));
}
