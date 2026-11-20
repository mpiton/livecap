//! End-to-end tests: the agent binary, a fake Gladia and `livekit-server --dev`.
//!
//! Skipped (with a message) when `LIVEKIT_SERVER_BIN` is not set.

mod common;

use std::time::Duration;

use common::fake_gladia::{FakeGladia, TRANSCRIPT, TRANSLATION};
use common::livekit::{AgentProcess, Listener, LiveKitServer, Speaker, require_livekit};

const CAPTIONS: &str = "livecap.captions";
const WAIT: Duration = Duration::from_secs(20);

/// A speaker, a listener and the agent in `room`; returns them once the agent answered join.
async fn call(
    livekit: &LiveKitServer,
    agent: &AgentProcess,
    room: &str,
    attributes: &[(&str, &str)],
) -> (Speaker, Listener) {
    let listener = Listener::join(&livekit.url(), room, "listener", CAPTIONS).await;
    let speaker = Speaker::join(&livekit.url(), room, "speaker", attributes).await;
    let (status, body) = agent.join(room).await;
    assert!((200..300).contains(&status), "join answered {status} {body}");
    (speaker, listener)
}

#[tokio::test(flavor = "multi_thread")]
async fn captions_reach_the_listener_on_the_captions_topic() {
    let Some(bin) = require_livekit("captions_reach_the_listener_on_the_captions_topic") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "captions",
        &[("livecap.lang", "en"), ("livecap.target", "fr")],
    )
    .await;

    let caption = listener.next(WAIT).await.expect("no caption arrived");
    assert_eq!(caption["kind"], "translation");
    assert_eq!(caption["text"], TRANSLATION);
    assert_eq!(caption["original"], TRANSCRIPT);
    assert_eq!(caption["speaker"], "speaker");
    let calls = gladia.calls();
    assert_eq!(calls.opens.len(), 1);
    assert!(calls.audio.iter().any(|(_, bytes)| *bytes > 0));

    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_transcript_or_translation_text_reaches_the_logs() {
    let Some(bin) = require_livekit("no_transcript_or_translation_text_reaches_the_logs") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "quiet-logs",
        &[("livecap.lang", "en"), ("livecap.target", "fr")],
    )
    .await;
    // Several utterances go through, then the speaker leaves: every code path that logs runs.
    for _ in 0..3 {
        assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    }
    speaker.leave().await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    listener.leave().await;

    let logs = agent.logs();
    assert!(logs.contains("captioning"), "the agent logged nothing useful:\n{logs}");
    for word in ["Quokka", "quokka", "seventy", "soixante"] {
        assert!(!logs.contains(word), "`{word}` reached the logs:\n{logs}");
    }
}
