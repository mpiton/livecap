//! Joins a LiveKit room as a hidden agent and captions every speaker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use livekit::prelude::*;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use livekit_api::access_token::{AccessToken, VideoGrants};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::audio::{Chunker, SAMPLE_RATE};
use crate::config::Config;
use crate::filter::is_suspect;
use crate::gladia::{CaptionEvent, CaptionKind, GladiaClient, parse_languages, run_session};

/// Data topic carrying captions to the people listening to a speaker.
pub const TOPIC_CAPTIONS: &str = "livecap.captions";
/// Data topic carrying a speaker's own live transcript back to them.
pub const TOPIC_SELF: &str = "livecap.self";

/// Audio chunks buffered per speaker before older ones are dropped (about 4 s).
const AUDIO_BUFFER: usize = 100;

/// How long `join` waits for LiveKit before answering `join-failed`. Callers wait for the answer
/// before telling people whether captions will come, so this stays short.
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout of one call to LiveKit's server API.
const LIVEKIT_API_TIMEOUT: Duration = Duration::from_secs(5);

/// How long [`Agent::shutdown`] waits for the open Gladia sessions to be deleted. Covers the
/// last results (3 s) and the deletion retries (about 15 s).
const SESSIONS_DELETION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Agent {
    config: Config,
    gladia: GladiaClient,
    http: reqwest::Client,
    /// Rooms followed or being joined, with the state of their join.
    rooms: Mutex<HashMap<String, watch::Receiver<RoomState>>>,
    rooms_changed: Notify,
    sessions: Arc<Gauge>,
    /// Set to `true` to make every room leave now.
    leave: watch::Sender<bool>,
}

/// Why the agent did not join a room; [`JoinError::reason`] is the short code `join` answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinError {
    /// `LIVECAP_MAX_ROOMS` rooms are already followed.
    Full,
    /// The room does not exist, or LiveKit refused or did not answer in time.
    JoinFailed,
}

impl JoinError {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::JoinFailed => "join-failed",
        }
    }
}

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for JoinError {}

/// Load reported by `/health`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub rooms: usize,
    pub sessions: usize,
    pub max_rooms: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoomState {
    Connecting,
    Joined,
    Failed,
}

/// A count that can be awaited until it drops to zero.
#[derive(Debug, Default)]
pub struct Gauge {
    count: AtomicUsize,
    changed: Notify,
}

impl Gauge {
    pub fn get(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn increment(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    fn decrement(&self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    /// Waits until the count is zero.
    pub async fn zero(&self) {
        loop {
            let changed = self.changed.notified();
            if self.get() == 0 {
                return;
            }
            changed.await;
        }
    }
}

/// What the browser receives, as JSON, on [`TOPIC_CAPTIONS`] and [`TOPIC_SELF`].
#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CaptionPayload<'a> {
    pub id: &'a str,
    pub speaker: &'a str,
    pub kind: &'static str,
    pub text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original: Option<&'a str>,
    pub lang: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_lang: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    pub suspect: bool,
}

struct Speaker {
    pump: JoinHandle<()>,
}

impl Agent {
    pub fn new(config: Config) -> Arc<Self> {
        let gladia = GladiaClient::new(
            &config.gladia_api_url,
            &config.gladia_api_key,
            &config.gladia_region,
            config.speech_threshold,
        );
        Arc::new(Self {
            config,
            gladia,
            http: reqwest::Client::new(),
            rooms: Mutex::new(HashMap::new()),
            rooms_changed: Notify::new(),
            sessions: Arc::new(Gauge::default()),
            leave: watch::channel(false).0,
        })
    }

    pub fn identity(&self) -> &str {
        &self.config.agent_identity
    }

    pub async fn load(&self) -> Load {
        Load {
            rooms: self.rooms.lock().await.len(),
            sessions: self.sessions.get(),
            max_rooms: self.config.max_rooms,
        }
    }

    /// Waits until the agent follows no room.
    pub async fn idle(&self) {
        loop {
            let changed = self.rooms_changed.notified();
            if self.rooms.lock().await.is_empty() {
                return;
            }
            changed.await;
        }
    }

    /// Leaves every room now and waits until their Gladia sessions are deleted.
    pub async fn shutdown(&self) {
        self.leave.send_replace(true);
        if tokio::time::timeout(SESSIONS_DELETION_TIMEOUT, self.sessions.zero())
            .await
            .is_err()
        {
            warn!(
                sessions = self.sessions.get(),
                "Gladia sessions still open at exit"
            );
        }
    }

    /// Enters `room` and captions it in the background until the last person leaves.
    ///
    /// Returns once the agent is in the room. A room already followed (or being joined) is not
    /// joined twice: the call waits for that join and shares its outcome.
    ///
    /// # Errors
    ///
    /// [`JoinError::Full`] at the room cap; [`JoinError::JoinFailed`] when the room does not
    /// exist or LiveKit refuses or does not answer within [`JOIN_TIMEOUT`].
    pub async fn join(self: &Arc<Self>, room: &str) -> Result<(), JoinError> {
        let state = {
            let mut rooms = self.rooms.lock().await;
            if let Some(state) = rooms.get(room) {
                let mut state = state.clone();
                drop(rooms);
                return match state.wait_for(|s| *s != RoomState::Connecting).await {
                    Ok(s) if *s == RoomState::Joined => Ok(()),
                    _ => Err(JoinError::JoinFailed),
                };
            }
            if rooms.len() >= self.config.max_rooms {
                warn!(
                    room,
                    max_rooms = self.config.max_rooms,
                    "room cap reached, not joining"
                );
                return Err(JoinError::Full);
            }
            let (state, receiver) = watch::channel(RoomState::Connecting);
            rooms.insert(room.to_owned(), receiver);
            state
        };

        let connected = match tokio::time::timeout(JOIN_TIMEOUT, self.connect(room)).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("no answer within {JOIN_TIMEOUT:?}")),
        };
        match connected {
            Ok((lk_room, events)) => {
                state.send_replace(RoomState::Joined);
                let agent = Arc::clone(self);
                let room = room.to_owned();
                tokio::spawn(async move {
                    agent.stay(&room, lk_room, events).await;
                    agent.forget(&room).await;
                    drop(state);
                });
                Ok(())
            }
            Err(err) => {
                warn!(room, error = %format!("{err:#}"), "cannot join the room");
                state.send_replace(RoomState::Failed);
                self.forget(room).await;
                Err(JoinError::JoinFailed)
            }
        }
    }

    async fn forget(&self, room: &str) {
        self.rooms.lock().await.remove(room);
        self.rooms_changed.notify_waiters();
    }

    /// Connects to an existing room. LiveKit creates missing rooms on join, so existence is
    /// checked first: the agent must not open a room nobody is in.
    async fn connect(&self, room: &str) -> Result<(Room, mpsc::UnboundedReceiver<RoomEvent>)> {
        if !self.room_exists(room).await? {
            anyhow::bail!("the room does not exist");
        }
        let token = AccessToken::with_api_key(
            &self.config.livekit_api_key,
            &self.config.livekit_api_secret,
        )
        .with_identity(&self.config.agent_identity)
        .with_name("Captions")
        .with_ttl(Duration::from_secs(6 * 3600))
        .with_grants(VideoGrants {
            room_join: true,
            room: room.to_owned(),
            can_publish: Some(false),
            can_subscribe: Some(true),
            can_publish_data: Some(true),
            hidden: true,
            agent: true,
            ..Default::default()
        })
        .to_jwt()
        .context("cannot sign the agent token")?;
        Room::connect(&self.config.livekit_url, &token, RoomOptions::default())
            .await
            .context("cannot join the LiveKit room")
    }

    /// Asks LiveKit's `RoomService.ListRooms` (Twirp, JSON) whether `room` exists.
    async fn room_exists(&self, room: &str) -> Result<bool> {
        #[derive(Deserialize)]
        struct Listed {
            #[serde(default)]
            rooms: Vec<Named>,
        }
        #[derive(Deserialize)]
        struct Named {
            #[serde(default)]
            name: String,
        }
        let token = AccessToken::with_api_key(
            &self.config.livekit_api_key,
            &self.config.livekit_api_secret,
        )
        .with_ttl(Duration::from_secs(60))
        .with_grants(VideoGrants {
            room_list: true,
            ..Default::default()
        })
        .to_jwt()
        .context("cannot sign the API token")?;
        let listed: Listed = self
            .http
            .post(format!(
                "{}/twirp/livekit.RoomService/ListRooms",
                http_base(&self.config.livekit_url)
            ))
            .bearer_auth(token)
            .json(&serde_json::json!({ "names": [room] }))
            .timeout(LIVEKIT_API_TIMEOUT)
            .send()
            .await
            .context("LiveKit unreachable")?
            .error_for_status()
            .context("LiveKit refused to list rooms")?
            .json()
            .await
            .context("unexpected LiveKit answer")?;
        Ok(listed.rooms.iter().any(|r| r.name == room))
    }

    /// Captions `room` until the last person leaves, the room ends or the agent stops.
    async fn stay(
        &self,
        room_name: &str,
        room: Room,
        mut events: mpsc::UnboundedReceiver<RoomEvent>,
    ) {
        let room = Arc::new(room);
        info!(room = room_name, "agent joined");

        let mut speakers: HashMap<ParticipantIdentity, Speaker> = HashMap::new();
        let mut leave = self.leave.subscribe();
        loop {
            let event = tokio::select! {
                event = events.recv() => event,
                _ = leave.wait_for(|leave| *leave) => None,
            };
            let Some(event) = event else { break };
            match event {
                RoomEvent::TrackSubscribed {
                    track: RemoteTrack::Audio(track),
                    participant,
                    ..
                } => {
                    let identity = participant.identity();
                    if let Some(old) = speakers.remove(&identity) {
                        old.pump.abort();
                    }
                    match self
                        .start_speaker(Arc::clone(&room), &participant, track)
                        .await
                    {
                        Ok(speaker) => {
                            speakers.insert(identity, speaker);
                        }
                        Err(err) => {
                            warn!(speaker = %identity.0, error = %format!("{err:#}"), "no captions")
                        }
                    }
                }
                RoomEvent::TrackUnsubscribed {
                    track: RemoteTrack::Audio(_),
                    participant,
                    ..
                } => {
                    if let Some(old) = speakers.remove(&participant.identity()) {
                        old.pump.abort();
                    }
                }
                RoomEvent::ParticipantDisconnected(participant) => {
                    if let Some(old) = speakers.remove(&participant.identity()) {
                        old.pump.abort();
                    }
                    if room.remote_participants().is_empty() {
                        break;
                    }
                }
                RoomEvent::Disconnected { .. } => break,
                _ => {}
            }
        }

        for speaker in speakers.into_values() {
            speaker.pump.abort();
        }
        room.close().await.ok();
        info!(room = room_name, "agent left");
    }

    async fn start_speaker(
        &self,
        room: Arc<Room>,
        participant: &RemoteParticipant,
        track: RemoteAudioTrack,
    ) -> Result<Speaker> {
        let identity = participant.identity();
        let attributes = participant.attributes();
        // The attribute values are never logged: only whether they are usable.
        let languages = attributes
            .get(&self.config.lang_attribute)
            .and_then(|raw| parse_languages(raw))
            .with_context(|| {
                format!(
                    "participant has no valid `{}` attribute",
                    self.config.lang_attribute
                )
            })?;
        let target = match attributes.get(&self.config.target_attribute) {
            None => None,
            Some(raw) => match parse_languages(raw).as_deref() {
                Some([one]) => Some(one.clone()),
                _ => {
                    warn!(speaker = %identity.0, attribute = %self.config.target_attribute, "invalid target language, captions not translated");
                    None
                }
            },
        };
        let lang = languages.join(",");

        let session = self.gladia.open(&languages, target.as_deref()).await?;
        info!(speaker = %identity.0, %lang, target = target.as_deref().unwrap_or("-"), session = %session.id, "captioning");
        let translate = session.translate;

        let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_BUFFER);
        let (event_tx, event_rx) = mpsc::channel::<CaptionEvent>(64);

        // The session ends when the audio channel closes (speaker gone, room left) or on error;
        // either way it is then deleted. Only its id is logged.
        self.sessions.increment();
        let sessions = Arc::clone(&self.sessions);
        let gladia = self.gladia.clone();
        let session_id = session.id.clone();
        tokio::spawn(async move {
            if let Err(err) = run_session(session, audio_rx, event_tx).await {
                warn!(session = %session_id, error = %format!("{err:#}"), "Gladia session ended");
            }
            match gladia.delete(&session_id).await {
                Ok(()) => info!(session = %session_id, "Gladia session deleted"),
                Err(_) => warn!(session = %session_id, "Gladia session not deleted, giving up"),
            }
            sessions.decrement();
        });
        tokio::spawn(publish_captions(
            room,
            identity.clone(),
            lang,
            translate,
            event_rx,
        ));

        let pump = tokio::spawn(async move {
            let mut stream = NativeAudioStream::new(track.rtc_track(), SAMPLE_RATE as i32, 1);
            let mut chunker = Chunker::default();
            while let Some(frame) = stream.next().await {
                for chunk in chunker.push(&frame.data) {
                    // Network trouble upstream: drop audio rather than fall behind.
                    if audio_tx.try_send(chunk).is_err() && audio_tx.is_closed() {
                        return;
                    }
                }
            }
        });
        Ok(Speaker { pump })
    }
}

/// Sends a speaker's transcript back to them, and the translation (or the transcript) to everyone else.
async fn publish_captions(
    room: Arc<Room>,
    speaker: ParticipantIdentity,
    lang: String,
    translate: bool,
    mut events: mpsc::Receiver<CaptionEvent>,
) {
    let mut suspect_ids: HashSet<String> = HashSet::new();
    while let Some(event) = events.recv().await {
        let suspect = match event.kind {
            CaptionKind::Partial => false,
            CaptionKind::Final => {
                let s = is_suspect(&event.text);
                if s {
                    suspect_ids.insert(event.utterance_id.clone());
                }
                s
            }
            CaptionKind::Translation => {
                suspect_ids.contains(&event.utterance_id)
                    || event.original.as_deref().is_some_and(is_suspect)
            }
        };
        let payload = to_payload(&event, &speaker.0, &lang, suspect);
        let Ok(bytes) = serde_json::to_vec(&payload) else {
            continue;
        };

        let local = room.local_participant();
        if matches!(event.kind, CaptionKind::Partial | CaptionKind::Final) {
            let packet = DataPacket {
                payload: bytes.clone(),
                topic: Some(TOPIC_SELF.to_owned()),
                reliable: true,
                destination_identities: vec![speaker.clone()],
            };
            if let Err(err) = local.publish_data(packet).await {
                warn!(%err, "cannot send transcript to speaker");
            }
        }

        let for_listeners = match event.kind {
            CaptionKind::Translation => true,
            CaptionKind::Final => !translate,
            CaptionKind::Partial => false,
        };
        if !for_listeners || suspect {
            continue;
        }
        let listeners: Vec<ParticipantIdentity> = room
            .remote_participants()
            .into_keys()
            .filter(|id| *id != speaker)
            .collect();
        if listeners.is_empty() {
            continue;
        }
        let packet = DataPacket {
            payload: bytes,
            topic: Some(TOPIC_CAPTIONS.to_owned()),
            reliable: true,
            destination_identities: listeners,
        };
        if let Err(err) = local.publish_data(packet).await {
            warn!(%err, "cannot send captions");
        }
    }
}

/// HTTP base of LiveKit's server API, from the signalling URL (`wss://` becomes `https://`).
fn http_base(url: &str) -> String {
    let url = url.trim_end_matches('/');
    if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        url.to_owned()
    }
}

fn to_payload<'a>(
    event: &'a CaptionEvent,
    speaker: &'a str,
    lang: &'a str,
    suspect: bool,
) -> CaptionPayload<'a> {
    CaptionPayload {
        id: &event.utterance_id,
        speaker,
        kind: match event.kind {
            CaptionKind::Partial => "partial",
            CaptionKind::Final => "final",
            CaptionKind::Translation => "translation",
        },
        text: &event.text,
        original: event.original.as_deref(),
        lang,
        detected_lang: event.language.as_deref(),
        confidence: event.confidence,
        suspect,
    }
}

#[cfg(test)]
mod tests {
    use super::{http_base, to_payload};

    #[test]
    fn the_server_api_lives_on_the_http_side_of_the_signalling_url() {
        assert_eq!(
            http_base("wss://x.livekit.cloud/"),
            "https://x.livekit.cloud"
        );
        assert_eq!(http_base("ws://127.0.0.1:7880"), "http://127.0.0.1:7880");
        assert_eq!(http_base("https://lk.example"), "https://lk.example");
    }
    use crate::gladia::{CaptionEvent, CaptionKind};

    #[test]
    fn payload_uses_camel_case_and_skips_empty_fields() -> anyhow::Result<()> {
        let event = CaptionEvent {
            utterance_id: "u1".into(),
            kind: CaptionKind::Translation,
            text: "Je suis fatigué.".into(),
            original: Some("Vizaka aho.".into()),
            language: Some("mg".into()),
            confidence: None,
        };
        let json = serde_json::to_value(to_payload(&event, "aline", "mg", false))?;
        assert_eq!(json["kind"], "translation");
        assert_eq!(json["detectedLang"], "mg");
        assert_eq!(json["original"], "Vizaka aho.");
        assert!(json.get("confidence").is_none());
        Ok(())
    }
}
