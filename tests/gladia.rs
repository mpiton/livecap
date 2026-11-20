//! The Gladia client against the fake Gladia: what it sends when it opens and deletes sessions.

mod common;

use std::time::Duration;

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

#[tokio::test]
async fn deleting_a_session_calls_delete_with_its_id() {
    let gladia = FakeGladia::start().await;
    let client = GladiaClient::new(&gladia.url(), "key-1", "eu-west", 0.75);
    let session = client.open(&langs(&["fr"]), None).await.unwrap();
    client.delete(&session.id).await.unwrap();
    assert_eq!(gladia.calls().deleted, vec![session.id]);
}

#[tokio::test]
async fn a_refused_deletion_is_retried_until_it_succeeds() {
    let gladia = FakeGladia::start().await;
    let client = GladiaClient::new(&gladia.url(), "key-1", "eu-west", 0.75)
        .with_delete_retries(4, Duration::from_millis(20));
    gladia.refuse_next_deletes(2);
    client.delete("session-9").await.unwrap();
    let calls = gladia.calls();
    assert_eq!(calls.delete_attempts.len(), 3);
    assert_eq!(calls.deleted, vec!["session-9".to_owned()]);
}

#[tokio::test]
async fn deletion_gives_up_after_a_bounded_number_of_attempts() {
    let gladia = FakeGladia::start().await;
    let client = GladiaClient::new(&gladia.url(), "key-1", "eu-west", 0.75)
        .with_delete_retries(3, Duration::from_millis(20));
    gladia.refuse_next_deletes(100);
    let started = std::time::Instant::now();
    assert!(client.delete("session-9").await.is_err());
    assert_eq!(gladia.calls().delete_attempts.len(), 3);
    // Delays grow: 20 ms, then 40 ms.
    assert!(started.elapsed() >= Duration::from_millis(60));
}
