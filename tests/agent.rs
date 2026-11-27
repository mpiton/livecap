//! End-to-end tests: the agent binary, the Azure fakes and `livekit-server --dev`.
//!
//! Skipped (with a message) when `LIVEKIT_SERVER_BIN` is not set.

mod common;

use std::time::Duration;

use common::fake_speech::{Phrase, TRANSCRIPT};
use common::fake_translator::TRANSLATION;
use common::livekit::{
    AgentProcess, Listener, LiveKitServer, Speaker, require_livekit, wait_until,
};
use common::{Azure, SPEECH_KEY, TRANSLATOR_KEY, TRANSLATOR_REGION};

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

/// True once every Azure Speech connection the agent opened is over.
fn all_closed(azure: &Azure) -> bool {
    let connections = azure.speech.connections();
    !connections.is_empty() && connections.iter().all(|c| c.closed)
}

#[tokio::test(flavor = "multi_thread")]
async fn captions_reach_the_listener_on_the_captions_topic() {
    let Some(bin) = require_livekit("captions_reach_the_listener_on_the_captions_topic") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "captions",
        &[("livecap.lang", "en-US"), ("livecap.target", "fr")],
    )
    .await;

    let caption = listener.next(WAIT).await.expect("no caption arrived");
    assert_eq!(caption["kind"], "translation");
    assert_eq!(caption["text"], TRANSLATION);
    assert_eq!(caption["original"], TRANSCRIPT);
    assert_eq!(caption["speaker"], "speaker");
    assert_eq!(caption["lang"], "en-US");
    assert_eq!(caption["detectedLang"], "en-US");
    assert_eq!(caption["suspect"], false);
    let connections = azure.speech.connections();
    assert_eq!(connections.len(), 1);
    assert!(connections[0].audio_bytes > 0 && connections[0].riff);

    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_speaker_reads_partials_and_finals_on_the_self_topic() {
    let Some(bin) = require_livekit("the_speaker_reads_partials_and_finals_on_the_self_topic")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (mut speaker, listener) = call(
        &livekit,
        &agent,
        "self",
        &[("livecap.lang", "en-US"), ("livecap.target", "fr")],
    )
    .await;
    let partial = speaker.next_transcript(WAIT).await.expect("no partial");
    let fin = speaker.next_transcript(WAIT).await.expect("no final");
    assert_eq!(partial["kind"], "partial");
    assert_eq!(fin["kind"], "final");
    assert_eq!(fin["text"], TRANSCRIPT);
    assert_eq!(fin["id"], partial["id"]);
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_text_and_no_key_reach_the_logs() {
    let Some(bin) = require_livekit("no_text_and_no_key_reach_the_logs") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "quiet-logs",
        &[("livecap.lang", "en-US"), ("livecap.target", "fr")],
    )
    .await;
    // Several utterances go through, a translation fails, Azure cuts the connection once, then
    // the speaker leaves: every code path that logs runs.
    for _ in 0..3 {
        assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    }
    azure.translator.set_failing(true);
    wait_until(WAIT, || async {
        agent.logs().contains("translation failed")
    })
    .await;
    azure.translator.set_failing(false);
    azure.speech.cut_next(1);
    wait_until(WAIT, || async { azure.speech.connections().len() >= 2 }).await;
    assert!(
        listener.next(WAIT).await.is_some(),
        "no caption after the cut"
    );
    speaker.leave().await;
    wait_until(WAIT, || async { all_closed(&azure) }).await;
    listener.leave().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let logs = agent.logs();
    assert!(
        logs.contains("captioning"),
        "the agent logged nothing useful:\n{logs}"
    );
    for word in [
        "Quokka",
        "quokka",
        "seventy",
        "soixante",
        SPEECH_KEY,
        TRANSLATOR_KEY,
    ] {
        assert!(!logs.contains(word), "`{word}` reached the logs:\n{logs}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_list_of_locales_opens_one_connection_detecting_among_them() {
    let Some(bin) = require_livekit("a_list_of_locales_opens_one_connection_detecting_among_them")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "languages",
        &[("livecap.lang", "fr-fr, EN-US"), ("livecap.target", "de")],
    )
    .await;
    let caption = listener.next(WAIT).await.expect("no caption arrived");
    assert_eq!(caption["lang"], "fr-FR,en-US");

    let connections = azure.speech.connections();
    assert_eq!(connections.len(), 1);
    let c = &connections[0];
    assert_eq!(c.path, "/stt/speech/universal/v2");
    assert_eq!(c.query, "format=detailed&lidEnabled=true&profanity=raw");
    assert_eq!(
        c.headers
            .get("ocp-apim-subscription-key")
            .map(String::as_str),
        Some(SPEECH_KEY)
    );
    assert_eq!(
        c.contexts[0].body["languageId"]["languages"],
        serde_json::json!(["fr-FR", "en-US"])
    );
    assert_eq!(c.contexts[0].body["languageId"]["mode"], "DetectContinuous");
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_locale_list_leaves_the_track_uncaptioned_and_the_agent_running() {
    let Some(bin) = require_livekit("an_invalid_locale_list_leaves_the_track_uncaptioned") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, listener) = call(
        &livekit,
        &agent,
        "bad-language",
        &[
            ("livecap.lang", "fr-FR,zz-Weird value"),
            ("livecap.target", "en"),
        ],
    )
    .await;
    wait_until(WAIT, || async { agent.logs().contains("no captions") }).await;
    assert!(azure.speech.connections().is_empty());
    let logs = agent.logs();
    assert!(
        !logs.contains("Weird"),
        "the attribute value reached the logs:\n{logs}"
    );

    // An empty attribute and a list of 11 locales: still no connection, and the agent answers.
    let eleven = "ar-EG,de-DE,en-US,es-ES,fr-FR,it-IT,ja-JP,ko-KR,pt-BR,zh-CN,ru-RU";
    let empty = Speaker::join(
        &livekit.url(),
        "bad-language",
        "empty",
        &[("livecap.lang", " ")],
    )
    .await;
    let long = Speaker::join(
        &livekit.url(),
        "bad-language",
        "long",
        &[("livecap.lang", eleven)],
    )
    .await;
    wait_until(WAIT, || async {
        agent.logs().matches("no captions").count() >= 3
    })
    .await;
    assert!(azure.speech.connections().is_empty());
    assert!(
        !agent.logs().contains("ru-RU"),
        "the attribute value reached the logs"
    );
    assert_eq!(
        reqwest::get(format!("{}/health", agent.base_url))
            .await
            .unwrap()
            .status(),
        200
    );

    long.leave().await;
    empty.leave().await;
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn suspect_finals_never_reach_the_listeners_nor_the_translator() {
    let Some(bin) = require_livekit("suspect_finals_never_reach_the_listeners_nor_the_translator")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    // A Malagasy speaker who declared French and English: Azure detects an undeclared language.
    azure.speech.set_phrase(Phrase {
        language: Some("mg-MG"),
        ..Phrase::default()
    });
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (mut speaker, mut listener) = call(
        &livekit,
        &agent,
        "suspect",
        &[("livecap.lang", "fr-FR,en-US"), ("livecap.target", "de")],
    )
    .await;

    let mut finals = Vec::new();
    while finals.len() < 2 {
        let message = speaker.next_transcript(WAIT).await.expect("no transcript");
        if message["kind"] == "final" {
            finals.push(message);
        }
    }
    assert!(finals.iter().all(|f| f["suspect"] == true), "{finals:?}");
    // Then a low confidence.
    azure.speech.set_phrase(Phrase {
        language: Some("fr-FR"),
        confidence: 0.2,
        ..Phrase::default()
    });
    let unconfident = loop {
        let message = speaker.next_transcript(WAIT).await.expect("no transcript");
        if message["kind"] == "final" && message["detectedLang"] == "fr-FR" {
            break message;
        }
    };
    assert_eq!(unconfident["suspect"], true, "{unconfident}");
    assert!(listener.next(Duration::from_secs(2)).await.is_none());
    assert!(azure.translator.requests().is_empty());
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn translation_is_skipped_when_the_listener_reads_the_spoken_language() {
    let Some(bin) = require_livekit("translation_is_skipped_when_the_listener_reads_the_spoken")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "same-language",
        &[("livecap.lang", "en-US"), ("livecap.target", "en")],
    )
    .await;
    let caption = listener.next(WAIT).await.expect("no caption arrived");
    assert_eq!(caption["kind"], "final");
    assert_eq!(caption["text"], TRANSCRIPT);
    assert!(azure.translator.requests().is_empty());
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finals_are_translated_with_the_detected_language_and_the_target() {
    let Some(bin) = require_livekit("finals_are_translated_with_the_detected_language") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "translated",
        &[("livecap.lang", "fr-FR,en-US"), ("livecap.target", "fr")],
    )
    .await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    let requests = azure.translator.requests();
    let r = requests.first().expect("no translation request");
    assert_eq!(r.query.as_deref(), Some("api-version=3.0&from=en&to=fr"));
    assert_eq!(r.key.as_deref(), Some(TRANSLATOR_KEY));
    assert_eq!(r.region.as_deref(), Some(TRANSLATOR_REGION));
    assert_eq!(r.body, serde_json::json!([{ "Text": TRANSCRIPT }]));

    // Translator fails: no caption at all, the partials and finals still reach the speaker.
    azure.translator.set_failing(true);
    tokio::time::sleep(Duration::from_millis(500)).await;
    listener.clear();
    assert!(listener.next(Duration::from_secs(3)).await.is_none());
    azure.translator.set_failing(false);
    assert!(
        listener.next(WAIT).await.is_some(),
        "captions did not come back"
    );
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_connection_is_closed_when_its_speaker_leaves() {
    let Some(bin) = require_livekit("the_connection_is_closed_when_its_speaker_leaves") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) =
        call(&livekit, &agent, "leaving", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    assert!(!all_closed(&azure));
    speaker.leave().await;
    wait_until(WAIT, || async { all_closed(&azure) }).await;
    assert!(azure.speech.connections()[0].end_of_audio);
    wait_until(WAIT, || async {
        agent.logs().contains("Azure Speech session closed")
    })
    .await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_agent_reconnects_when_azure_cuts_the_connection_mid_call() {
    let Some(bin) = require_livekit("the_agent_reconnects_when_azure_cuts_the_connection") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    azure.speech.cut_next(2);
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) = call(&livekit, &agent, "cut", &[("livecap.lang", "en-US")]).await;
    wait_until(WAIT, || async { azure.speech.connections().len() >= 3 }).await;
    listener.clear();
    assert!(
        listener.next(WAIT).await.is_some(),
        "no caption after the cuts"
    );
    let connections = azure.speech.connections();
    assert!(connections[0].cut && connections[1].cut);
    assert!(connections[2].config.is_some() && connections[2].riff);
    assert_eq!(agent.health().await["rooms"], 1);
    assert_eq!(agent.health().await["sessions"], 1);
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_the_agent_closes_the_connections_still_open() {
    let Some(bin) = require_livekit("stopping_the_agent_closes_the_connections_still_open") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let mut agent = AgentProcess::start(
        &livekit.url(),
        &azure,
        &[("LIVECAP_DRAIN_TIMEOUT_SECS", "1")],
    )
    .await;

    let (speaker, mut listener) =
        call(&livekit, &agent, "stopping", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    agent.sigterm();
    let status = agent.wait_exit(WAIT).await.expect("the agent did not exit");
    assert!(status.success(), "exit status {status}");
    assert!(all_closed(&azure));
    assert!(azure.speech.connections()[0].end_of_audio);
    speaker.leave().await;
    listener.leave().await;
}

/// The agent pointed at an address where no LiveKit server listens.
async fn agent_without_livekit(extra: &[(&str, &str)]) -> (Azure, AgentProcess) {
    let azure = Azure::start().await;
    let nowhere = format!("ws://127.0.0.1:{}", common::livekit::free_tcp_port());
    let agent = AgentProcess::start(&nowhere, &azure, extra).await;
    (azure, agent)
}

#[tokio::test]
async fn join_refuses_a_missing_or_wrong_secret_and_a_bad_room_name() {
    let (_azure, agent) = agent_without_livekit(&[]).await;
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
    let (_azure, agent) = agent_without_livekit(&[]).await;
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
    let (_azure, agent) = agent_without_livekit(&[("LIVECAP_MAX_ROOMS", "4")]).await;
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
    let (_azure, agent) = agent_without_livekit(&[]).await;
    assert_eq!(agent.health().await["maxRooms"], 15);
}

#[tokio::test(flavor = "multi_thread")]
async fn join_answers_200_once_the_agent_is_in_the_room() {
    let Some(bin) = require_livekit("join_answers_200_once_the_agent_is_in_the_room") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
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
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
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
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[("LIVECAP_MAX_ROOMS", "1")]).await;
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
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

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
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;

    let (speaker, mut listener) = call(
        &livekit,
        &agent,
        "secret-room-name",
        &[("livecap.lang", "en-US")],
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

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_refuses_new_rooms_and_lets_the_current_call_finish() {
    let Some(bin) = require_livekit("sigterm_refuses_new_rooms_and_lets_the_current_call_finish")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let mut agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) =
        call(&livekit, &agent, "ongoing", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");
    let other = Listener::join(&livekit.url(), "newcomer", "listener", CAPTIONS).await;

    agent.sigterm();
    wait_until(WAIT, || async {
        agent.health().await["status"] == "draining"
    })
    .await;
    let (status, body) = agent.join("newcomer").await;
    assert_eq!(status, 503);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "error": "draining" })
    );
    // The call goes on: captions keep coming, the connection stays open, the agent is still there.
    listener.clear();
    assert!(
        listener.next(WAIT).await.is_some(),
        "captions stopped while draining"
    );
    assert!(!all_closed(&azure));
    assert!(agent.wait_exit(Duration::from_secs(2)).await.is_none());

    speaker.leave().await;
    listener.leave().await;
    let status = agent
        .wait_exit(WAIT)
        .await
        .expect("the agent did not exit after the call");
    assert!(status.success(), "exit status {status}");
    assert!(all_closed(&azure));
    other.leave().await;
}

/// Kills the LiveKit server for `length`, starts it again, and brings the two people back.
async fn outage(
    livekit: &mut LiveKitServer,
    speaker: Speaker,
    listener: Listener,
    room: &str,
    length: Duration,
) -> (Speaker, Listener) {
    livekit.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), speaker.leave()).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), listener.leave()).await;
    tokio::time::sleep(length).await;
    livekit.restart().await;
    let listener = Listener::join(&livekit.url(), room, "listener", CAPTIONS).await;
    let speaker = Speaker::join(
        &livekit.url(),
        room,
        "speaker",
        &[("livecap.lang", "en-US")],
    )
    .await;
    (speaker, listener)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_agent_comes_back_to_the_room_after_each_livekit_outage() {
    let Some(bin) = require_livekit("the_agent_comes_back_to_the_room_after_each_livekit_outage")
    else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let mut livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (mut speaker, mut listener) =
        call(&livekit, &agent, "bumpy", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");

    // Two outages in a row: the second recovers as well as the first, so the retry count
    // started again from zero after the first recovery.
    for round in 1..=2 {
        (speaker, listener) = outage(
            &mut livekit,
            speaker,
            listener,
            "bumpy",
            Duration::from_secs(2),
        )
        .await;
        let caption = listener.next(Duration::from_secs(60)).await;
        assert!(
            caption.is_some(),
            "no caption after outage {round}:\n{}",
            agent.logs()
        );
        assert_eq!(agent.health().await["rooms"], 1);
    }
    speaker.leave().await;
    listener.leave().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_room_gone_after_an_outage_stops_the_retries_without_errors() {
    let Some(bin) = require_livekit("a_room_gone_after_an_outage_stops_the_retries") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let mut livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) =
        call(&livekit, &agent, "vanished", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");

    livekit.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(5), speaker.leave()).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), listener.leave()).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    livekit.restart().await;

    // Nobody comes back. The SDK's own reconnection may recreate the room with the agent alone
    // in it (LiveKit then closes it after its empty timeout): the agent leaves it.
    wait_until(Duration::from_secs(90), || async {
        agent.health().await["rooms"] == 0
    })
    .await;
    wait_until(WAIT, || async { all_closed(&azure) }).await;
    assert!(
        !livekit
            .participants("vanished")
            .await
            .contains(&"livecap".to_owned())
    );
    // The LiveKit SDK logs its own retries as errors; the agent logs none.
    let logs = agent.logs();
    let errors: Vec<&str> = logs
        .lines()
        .filter(|l| l.contains(" ERROR livecap"))
        .collect();
    assert!(errors.is_empty(), "errors logged:\n{}", errors.join("\n"));
    let after = logs.len();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let quiet = agent.logs()[after..]
        .lines()
        .filter(|l| l.contains("livecap"))
        .count();
    assert_eq!(
        quiet,
        0,
        "the agent keeps logging:\n{}",
        &agent.logs()[after..]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_agent_reconnects_itself_after_an_outage_longer_than_the_sdk_retries() {
    let Some(bin) = require_livekit("the_agent_reconnects_itself_after_a_long_outage") else {
        return;
    };
    let _serial = common::LIVEKIT_LOCK.lock().await;
    let mut livekit = LiveKitServer::start(bin).await;
    let azure = Azure::start().await;
    let agent = AgentProcess::start(&livekit.url(), &azure, &[]).await;
    let (speaker, mut listener) =
        call(&livekit, &agent, "long", &[("livecap.lang", "en-US")]).await;
    assert!(listener.next(WAIT).await.is_some(), "no caption arrived");

    // The LiveKit SDK gives up after about 45 s of retries; the agent keeps the room and
    // reconnects on its own once the server is back.
    let (speaker, mut listener) = outage(
        &mut livekit,
        speaker,
        listener,
        "long",
        Duration::from_secs(55),
    )
    .await;
    let caption = listener.next(Duration::from_secs(60)).await;
    let logs = agent.logs();
    assert!(caption.is_some(), "no caption after the outage:\n{logs}");
    assert!(
        logs.contains("back in the room"),
        "the agent did not reconnect itself:\n{logs}"
    );
    speaker.leave().await;
    listener.leave().await;
}
