//! The Gladia client against the fake Gladia: what it sends when it opens and deletes sessions.

mod common;

use common::fake_gladia::FakeGladia;
use livecap::gladia::GladiaClient;
use serde_json::{Value, json};

fn langs(codes: &[&str]) -> Vec<String> {
    codes.iter().map(|c| (*c).to_owned()).collect()
}

async fn opened_with(region: &str, languages: &[&str], target: Option<&str>) -> (Value, String) {
    let gladia = FakeGladia::start().await;
    let client = GladiaClient::new(&gladia.url(), "key-1", region, 0.75);
    client.open(&langs(languages), target).await.unwrap();
    let calls = gladia.calls();
    let open = calls.opens.first().unwrap();
    assert_eq!(open.api_key.as_deref(), Some("key-1"));
    (open.body.clone(), open.query.clone().unwrap_or_default())
}

fn expected_body(languages: &[&str], code_switching: bool, target: Option<&str>) -> Value {
    let mut body = json!({
        "encoding": "wav/pcm",
        "sample_rate": 16000,
        "bit_depth": 16,
        "channels": 1,
        "language_config": { "languages": languages, "code_switching": code_switching },
        "pre_processing": { "speech_threshold": 0.75 },
        "messages_config": {
            "receive_partial_transcripts": true,
            "receive_final_transcripts": true,
            "receive_realtime_processing_events": target.is_some()
        }
    });
    if let Some(target) = target {
        body["realtime_processing"] = json!({
            "translation": true,
            "translation_config": {
                "target_languages": [target],
                "context_adaptation": true,
                "informal": true
            }
        });
    }
    body
}

#[tokio::test]
async fn sessions_open_in_the_configured_region() {
    let (_, query) = opened_with("eu-west", &["fr"], None).await;
    assert_eq!(query, "region=eu-west");
    let (_, query) = opened_with("us-west", &["fr"], None).await;
    assert_eq!(query, "region=us-west");
}

#[tokio::test]
async fn one_language_opens_a_session_without_code_switching() {
    let (body, _) = opened_with("eu-west", &["mg"], Some("fr")).await;
    assert_eq!(body, expected_body(&["mg"], false, Some("fr")));
}

#[tokio::test]
async fn several_languages_turn_on_code_switching_with_those_languages_only() {
    let (body, _) = opened_with("eu-west", &["fr", "mg"], Some("en")).await;
    assert_eq!(body, expected_body(&["fr", "mg"], true, Some("en")));
}

#[tokio::test]
async fn a_reader_of_one_of_several_spoken_languages_still_gets_translations() {
    let (body, _) = opened_with("eu-west", &["fr", "mg"], Some("fr")).await;
    assert_eq!(body, expected_body(&["fr", "mg"], true, Some("fr")));
}

#[tokio::test]
async fn no_translation_when_the_reader_reads_the_only_spoken_language() {
    let (body, _) = opened_with("eu-west", &["fr"], Some("fr")).await;
    assert_eq!(body, expected_body(&["fr"], false, None));
    let (body, _) = opened_with("eu-west", &["fr"], None).await;
    assert_eq!(body, expected_body(&["fr"], false, None));
}
