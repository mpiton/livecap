//! End-to-end tests of the transcription switch: the `livecap.transcribe` participant attribute,
//! set by the application through `RoomService.UpdateParticipant`, stops and resumes the Azure
//! Speech connection of one speaker.
//!
//! Skipped (with a message) when `LIVEKIT_SERVER_BIN` is not set.

mod common;

use std::time::Duration;

use common::Azure;
use common::fake_speech::TRANSCRIPT;
use common::fake_translator::TRANSLATION;
use common::livekit::{
    AgentProcess, Listener, LiveKitServer, Speaker, require_livekit, wait_until,
};
use serde_json::Value;

const CAPTIONS: &str = "livecap.captions";
const TRANSCRIBE: &str = "livecap.transcribe";
const WAIT: Duration = Duration::from_secs(20);
/// The switch must close the connection within a second (mpiton/livecap#9).
const STOP_WITHIN: Duration = Duration::from_secs(1);

async fn call(
    livekit: &LiveKitServer,
    agent: &AgentProcess,
    room: &str,
    attributes: &[(&str, &str)],
) -> (Speaker, Listener) {
    let listener = Listener::join(&livekit.url(), room, "listener", CAPTIONS).await;
    let speaker = Speaker::join(&livekit.url(), room, "speaker", attributes).await;
    let (status, body) = agent.join(room).await;
    assert_eq!(status, 200, "join answered {status} {body}");
    (speaker, listener)
}

/// Next status message on the captions topic, skipping the captions sent before it.
async fn next_status(listener: &mut Listener) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let message = listener.next(left).await?;
        if message["kind"] == "status" {
            return Some(message);
        }
    }
}

/// Next caption (not a status message) on the captions topic.
async fn next_caption(listener: &mut Listener) -> Option<Value> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let message = listener.next(left).await?;
        if message["kind"] != "status" {
            return Some(message);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn turning_transcription_off_closes_the_connection_and_tells_the_listeners() {
    let Some(bin) = require_livekit("turning_transcription_off_closes_the_connection") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "off",
        &[("livecap.lang", "en-US"), ("livecap.target", "fr")],
    )
    .await;
    assert!(
        next_caption(&mut listener).await.is_some(),
        "no caption arrived"
    );
    assert_eq!(agent.health().await["sessions"], 1);

    livekit
        .set_attributes("off", "speaker", &[(TRANSCRIBE, "off")])
        .await;
    wait_until(STOP_WITHIN, || async {
        azure.speech.connections().iter().all(|c| c.closed)
    })
    .await;
    let status = next_status(&mut listener).await.expect("no status message");
    assert_eq!(
        status,
        serde_json::json!({ "kind": "status", "speaker": "speaker", "status": "stopped" })
    );

    // Nothing more reaches Azure: no audio on the closed connection, no new connection.
    let frames = azure.speech.connections()[0].audio_frames;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let connections = azure.speech.connections();
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].audio_frames, frames);
    assert_eq!(agent.health().await["sessions"], 0);
    assert!(
        listener.next(Duration::from_secs(1)).await.is_none(),
        "something was sent after the stop"
    );
    let logs = agent.logs();
    assert!(!logs.contains(TRANSCRIPT) && !logs.contains(TRANSLATION));
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn turning_transcription_back_on_opens_a_new_connection_and_captions_resume() {
    let Some(bin) = require_livekit("turning_transcription_back_on_resumes") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) =
        call(&livekit, &agent, "resume", &[("livecap.lang", "en-US")]).await;
    assert!(
        next_caption(&mut listener).await.is_some(),
        "no caption arrived"
    );

    livekit
        .set_attributes("resume", "speaker", &[(TRANSCRIBE, "off")])
        .await;
    assert_eq!(
        next_status(&mut listener).await.expect("no stop")["status"],
        "stopped"
    );
    livekit
        .set_attributes("resume", "speaker", &[(TRANSCRIBE, "on")])
        .await;
    let status = next_status(&mut listener).await.expect("no resume");
    assert_eq!(
        status,
        serde_json::json!({ "kind": "status", "speaker": "speaker", "status": "resumed" })
    );
    let caption = next_caption(&mut listener)
        .await
        .expect("captions did not come back");
    assert_eq!(caption["text"], TRANSCRIPT);
    let connections = azure.speech.connections();
    assert_eq!(connections.len(), 2);
    assert!(connections[0].closed && !connections[1].closed);
    assert!(connections[1].config.is_some() && connections[1].riff);
    assert_eq!(agent.health().await["sessions"], 1);
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_speaker_joining_with_transcription_off_opens_no_connection_until_it_is_removed() {
    let Some(bin) = require_livekit("a_speaker_joining_with_transcription_off") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "joined-off",
        &[("livecap.lang", "en-US"), (TRANSCRIBE, "off")],
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(azure.speech.connections().is_empty());
    assert_eq!(agent.health().await["sessions"], 0);
    assert!(listener.next(Duration::from_secs(1)).await.is_none());

    // No attribute means `on`.
    livekit
        .set_attributes("joined-off", "speaker", &[(TRANSCRIBE, "")])
        .await;
    assert_eq!(
        next_status(&mut listener).await.expect("no resume")["status"],
        "resumed"
    );
    assert!(
        next_caption(&mut listener).await.is_some(),
        "no caption after the resume"
    );
    assert_eq!(azure.speech.connections().len(), 1);
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_attribute_name_is_configurable() {
    let Some(bin) = require_livekit("the_attribute_name_is_configurable") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(
        &livekit.url(),
        &azure,
        &[("LIVECAP_TRANSCRIBE_ATTRIBUTE", "app.captions")],
    )
    .await;
    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "renamed",
        &[("livecap.lang", "en-US"), (TRANSCRIBE, "off")],
    )
    .await;
    assert!(
        next_caption(&mut listener).await.is_some(),
        "the default name was obeyed"
    );
    livekit
        .set_attributes("renamed", "speaker", &[("app.captions", "off")])
        .await;
    assert_eq!(
        next_status(&mut listener).await.expect("no stop")["status"],
        "stopped"
    );
    speaker.leave().await;
    listener.leave().await;
}
