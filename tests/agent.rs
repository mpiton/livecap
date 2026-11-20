//! End-to-end tests: the agent binary, a fake Gladia and `livekit-server --dev`.
//!
//! Skipped (with a message) when `LIVEKIT_SERVER_BIN` is not set.

mod common;

use std::time::Duration;

use common::fake_gladia::{Behavior, FakeGladia, TRANSCRIPT, TRANSLATION};
use common::livekit::{
    AgentProcess, Listener, LiveKitServer, Speaker, require_livekit, wait_until,
};

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
    assert_eq!(status, 200, "join answered {status} {body}");
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
    wait_until(WAIT, || async { !gladia.calls().deleted.is_empty() }).await;
    listener.leave().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let logs = agent.logs();
    assert!(
        logs.contains("captioning"),
        "the agent logged nothing useful:\n{logs}"
    );
    for word in ["Quokka", "quokka", "seventy", "soixante"] {
        assert!(!logs.contains(word), "`{word}` reached the logs:\n{logs}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_of_spoken_languages_opens_one_code_switching_session_in_the_region() {
    let Some(bin) = require_livekit("a_list_of_spoken_languages_opens_one_code_switching_session")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(
        &livekit.url(),
        &gladia.url(),
        &[("GLADIA_REGION", "us-west")],
    )
    .await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "languages",
        &[("livecap.lang", "fr, MG"), ("livecap.target", "en")],
    )
    .await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");

    let calls = gladia.calls();
    assert_eq!(calls.opens.len(), 1);
    let open = &calls.opens[0];
    assert_eq!(open.query.as_deref(), Some("region=us-west"));
    assert_eq!(
        open.body["language_config"],
        serde_json::json!({ "languages": ["fr", "mg"], "code_switching": true })
    );
    assert_eq!(
        open.body["realtime_processing"]["translation_config"]["target_languages"],
        serde_json::json!(["en"])
    );
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_language_leaves_the_track_uncaptioned_and_the_agent_running() {
    let Some(bin) = require_livekit("an_invalid_language_leaves_the_track_uncaptioned") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, listener) = call(
        &livekit,
        &agent,
        "bad-language",
        &[
            ("livecap.lang", "fr,zz-Weird value"),
            ("livecap.target", "en"),
        ],
    )
    .await;
    wait_until(WAIT, || async { agent.logs().contains("no captions") }).await;
    assert!(gladia.calls().opens.is_empty());
    let logs = agent.logs();
    assert!(
        !logs.contains("Weird"),
        "the attribute value reached the logs:\n{logs}"
    );

    // A second speaker with an empty attribute: still no session, and the agent still answers.
    let empty = Speaker::join(
        &livekit.url(),
        "bad-language",
        "empty",
        &[("livecap.lang", " ")],
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(gladia.calls().opens.is_empty());
    assert_eq!(
        reqwest::get(format!("{}/health", agent.base_url))
            .await
            .unwrap()
            .status(),
        200
    );

    empty.leave().await;
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gladia_session_is_deleted_when_its_speaker_leaves() {
    let Some(bin) = require_livekit("the_gladia_session_is_deleted_when_its_speaker_leaves") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, mut listener) =
        call(&livekit, &agent, "leaving", &[("livecap.lang", "en")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    assert!(gladia.calls().deleted.is_empty());
    speaker.leave().await;
    wait_until(WAIT, || async { gladia.calls().deleted == ["session-1"] }).await;
    let logs = agent.logs();
    assert!(
        logs.contains("session-1"),
        "the session id is not logged:\n{logs}"
    );
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gladia_session_is_deleted_after_gladia_fails_it() {
    let Some(bin) = require_livekit("the_gladia_session_is_deleted_after_gladia_fails_it") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    gladia.set_behavior(Behavior::CloseWithError);
    gladia.refuse_next_deletes(1);
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, listener) = call(&livekit, &agent, "failing", &[("livecap.lang", "en")]).await;
    wait_until(WAIT, || async { gladia.calls().deleted == ["session-1"] }).await;
    assert_eq!(
        gladia.calls().delete_attempts.len(),
        2,
        "the refused deletion was not retried"
    );
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_the_agent_deletes_the_sessions_still_open() {
    let Some(bin) = require_livekit("stopping_the_agent_deletes_the_sessions_still_open") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let mut agent = AgentProcess::start(
        &livekit.url(),
        &gladia.url(),
        &[("LIVECAP_DRAIN_TIMEOUT_SECS", "1")],
    )
    .await;

    let (speaker, mut listener) =
        call(&livekit, &agent, "stopping", &[("livecap.lang", "en")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    agent.sigterm();
    let status = agent.wait_exit(WAIT).await.expect("the agent did not exit");
    assert!(status.success(), "exit status {status}");
    assert_eq!(gladia.calls().deleted, ["session-1"]);
    speaker.leave().await;
    listener.leave().await;
}

/// The agent pointed at an address where no LiveKit server listens.
async fn agent_without_livekit(extra: &[(&str, &str)]) -> (FakeGladia, AgentProcess) {
    let gladia = FakeGladia::start().await;
    let nowhere = format!("ws://127.0.0.1:{}", common::livekit::free_tcp_port());
    let agent = AgentProcess::start(&nowhere, &gladia.url(), extra).await;
    (gladia, agent)
}

#[tokio::test]
async fn join_refuses_a_missing_or_wrong_secret_and_a_bad_room_name() {
    let (_gladia, agent) = agent_without_livekit(&[]).await;
    let client = reqwest::Client::new();
    let url = format!("{}/rooms/room-1/join", agent.base_url);
    let none = client.post(&url).send().await.unwrap();
    assert_eq!(none.status(), 401);
    let wrong = client.post(&url).bearer_auth("nope").send().await.unwrap();
    assert_eq!(wrong.status(), 401);
    let (status, _) = agent.join("bad%20name").await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn join_answers_503_join_failed_when_livekit_is_unreachable() {
    let (_gladia, agent) = agent_without_livekit(&[]).await;
    let (status, body) = agent.join("room-1").await;
    assert_eq!(status, 503);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": "join-failed" })
    );
    assert_eq!(agent.health().await["rooms"], 0);
}

#[tokio::test]
async fn health_reports_version_load_and_cap_as_json() {
    let (_gladia, agent) = agent_without_livekit(&[("LIVECAP_MAX_ROOMS", "4")]).await;
    assert_eq!(
        agent.health().await,
        serde_json::json!({
            "status": "ok",
            "version": env!("CARGO_PKG_VERSION"),
            "rooms": 0,
            "sessions": 0,
            "maxRooms": 4
        })
    );
    let (_gladia, agent) = agent_without_livekit(&[]).await;
    assert_eq!(agent.health().await["maxRooms"], 15);
}

#[tokio::test(flavor = "multi_thread")]
async fn join_answers_200_once_the_agent_is_in_the_room() {
    let Some(bin) = require_livekit("join_answers_200_once_the_agent_is_in_the_room") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;
    let listener = Listener::join(&livekit.url(), "entered", "listener", CAPTIONS).await;

    let (status, body) = agent.join("entered").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "status": "joined" })
    );
    // No waiting: the agent is in the room when join answers.
    assert!(
        livekit
            .participants("entered")
            .await
            .contains(&"livecap".to_owned())
    );
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_join_on_a_followed_room_answers_200_without_a_duplicate() {
    let Some(bin) = require_livekit("a_second_join_on_a_followed_room_answers_200") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;
    let listener = Listener::join(&livekit.url(), "twice", "listener", CAPTIONS).await;

    let (first, second) = tokio::join!(agent.join("twice"), agent.join("twice"));
    assert_eq!((first.0, second.0), (200, 200));
    assert_eq!(agent.join("twice").await.0, 200);
    let agents = livekit
        .participants("twice")
        .await
        .into_iter()
        .filter(|p| p == "livecap")
        .count();
    assert_eq!(agents, 1);
    assert_eq!(agent.health().await["rooms"], 1);
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_answers_503_full_beyond_the_room_cap() {
    let Some(bin) = require_livekit("join_answers_503_full_beyond_the_room_cap") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent =
        AgentProcess::start(&livekit.url(), &gladia.url(), &[("LIVECAP_MAX_ROOMS", "1")]).await;
    let first = Listener::join(&livekit.url(), "first", "listener", CAPTIONS).await;
    let second = Listener::join(&livekit.url(), "second", "listener", CAPTIONS).await;

    assert_eq!(agent.join("first").await.0, 200);
    let (status, body) = agent.join("second").await;
    assert_eq!(status, 503);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": "full" })
    );
    assert!(
        !livekit
            .participants("second")
            .await
            .contains(&"livecap".to_owned())
    );
    first.leave().await;
    second.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn join_answers_503_for_a_room_that_does_not_exist_without_creating_it() {
    let Some(bin) = require_livekit("join_answers_503_for_a_room_that_does_not_exist") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (status, body) = agent.join("ghost").await;
    assert_eq!(status, 503);
    assert!(body.contains("join-failed"), "{body}");
    assert!(!livekit.room_names().await.contains(&"ghost".to_owned()));
    assert_eq!(agent.health().await["rooms"], 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn health_counts_rooms_and_sessions_without_naming_rooms() {
    let Some(bin) = require_livekit("health_counts_rooms_and_sessions_without_naming_rooms") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let gladia = FakeGladia::start().await;
    let agent = AgentProcess::start(&livekit.url(), &gladia.url(), &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "secret-room-name",
        &[("livecap.lang", "en")],
    )
    .await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    let health = agent.health().await;
    assert_eq!(health["rooms"], 1);
    assert_eq!(health["sessions"], 1);
    assert!(!health.to_string().contains("secret-room-name"));

    speaker.leave().await;
    wait_until(WAIT, || async { agent.health().await["sessions"] == 0 }).await;
    listener.leave().await;
    wait_until(WAIT, || async { agent.health().await["rooms"] == 0 }).await;
}
