//! Joins a LiveKit room as a hidden agent and captions every speaker.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
use crate::speech::{CaptionEvent, CaptionKind, SpeechClient, new_id, parse_locales};
use crate::translator::{TranslatorClient, parse_target, same_language, translator_code};

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

/// Reconnection attempts after the LiveKit SDK gave up on a connection (it retries for about
/// 45 s itself). With [`reconnect_delay`] they span about two minutes.
const RECONNECT_ATTEMPTS: u32 = 10;
/// Consecutive checks finding the room gone before the agent stops trying. More than one, so a
/// room that is being recreated by people reconnecting is not given up on.
const ROOM_GONE_CHECKS: u32 = 3;
/// How long the agent stays in a room with nobody else, e.g. one LiveKit recreated when the agent
/// reconnected. Matches LiveKit's default `departure_timeout`.
const ALONE_TIMEOUT: Duration = Duration::from_secs(20);

/// How long [`Agent::shutdown`] waits for the Azure Speech connections to close. Covers the last
/// results (3 s) and a reconnection delay in progress (8 s at most).
const SESSIONS_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

pub struct Agent {
    config: Config,
    speech: SpeechClient,
    translator: TranslatorClient,
    http: reqwest::Client,
    /// Rooms followed or being joined, with the state of their join.
    rooms: Mutex<HashMap<String, watch::Receiver<RoomState>>>,
    rooms_changed: Notify,
    sessions: Arc<Gauge>,
    /// Set once the agent is stopping: no new room.
    draining: AtomicBool,
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
    /// The agent is stopping: it finishes the current calls and takes no new room.
    Draining,
}

impl JoinError {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::JoinFailed => "join-failed",
            Self::Draining => "draining",
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
    pub draining: bool,
    pub rooms: usize,
    pub sessions: usize,
    pub max_rooms: usize,
}

/// How one LiveKit connection to a room ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ended {
    /// The call is over or the agent was told to leave.
    Finished,
    /// The connection dropped: try again while the room exists.
    Dropped,
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
        let speech = SpeechClient::new(
            &config.azure_speech_url,
            &config.azure_speech_key,
            config.azure_speech_min_confidence,
        );
        let translator = TranslatorClient::new(
            &config.azure_translator_url,
            &config.azure_translator_key,
            config.azure_translator_region.as_deref(),
        );
        Arc::new(Self {
            config,
            speech,
            translator,
            http: reqwest::Client::new(),
            rooms: Mutex::new(HashMap::new()),
            rooms_changed: Notify::new(),
            sessions: Arc::new(Gauge::default()),
            draining: AtomicBool::new(false),
            leave: watch::channel(false).0,
        })
    }

    pub fn identity(&self) -> &str {
        &self.config.agent_identity
    }

    pub async fn load(&self) -> Load {
        Load {
            draining: self.draining.load(Ordering::SeqCst),
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

    /// Stops taking rooms, lets the current calls finish for at most `deadline`, then leaves
    /// whatever is left and waits until the Azure Speech connections are closed.
    ///
    /// Resolves early, leaving at once, if `hurry` resolves first (a second stop signal).
    pub async fn drain(&self, deadline: Duration, hurry: impl Future<Output = ()>) {
        self.draining.store(true, Ordering::SeqCst);
        let load = self.load().await;
        info!(
            rooms = load.rooms,
            deadline_secs = deadline.as_secs(),
            "draining: no new room, waiting for the calls to end"
        );
        tokio::select! {
            result = tokio::time::timeout(deadline, self.idle()) => {
                if result.is_err() {
                    info!("drain deadline reached, leaving the remaining rooms");
                }
            }
            () = hurry => info!("second stop request, leaving the remaining rooms"),
        }
        self.shutdown().await;
    }

    /// Leaves every room now and waits until their Azure Speech connections are closed.
    pub async fn shutdown(&self) {
        self.leave.send_replace(true);
        if tokio::time::timeout(SESSIONS_CLOSE_TIMEOUT, self.sessions.zero())
            .await
            .is_err()
        {
            warn!(
                sessions = self.sessions.get(),
                "Azure Speech connections still open at exit"
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
        if self.draining.load(Ordering::SeqCst) {
            info!(room, "draining, not joining");
            return Err(JoinError::Draining);
        }
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

    /// Captions `room` until the last person leaves, the room ends or the agent stops, coming
    /// back after a LiveKit outage as long as the room exists.
    async fn stay(&self, room_name: &str, room: Room, events: mpsc::UnboundedReceiver<RoomEvent>) {
        let mut leave = self.leave.subscribe();
        let mut connection = Some((room, events));
        while let Some((room, events)) = connection.take() {
            if self.caption(room_name, room, events, &mut leave).await == Ended::Dropped {
                connection = self.reconnect(room_name, &mut leave).await;
            }
        }
    }

    /// Runs one LiveKit connection to `room` until it ends.
    async fn caption(
        &self,
        room_name: &str,
        room: Room,
        mut events: mpsc::UnboundedReceiver<RoomEvent>,
        leave: &mut watch::Receiver<bool>,
    ) -> Ended {
        let room = Arc::new(room);
        info!(room = room_name, "agent joined");

        let mut speakers: HashMap<ParticipantIdentity, Speaker> = HashMap::new();
        // Set while nobody else is in the room: the agent leaves when it expires.
        let mut alone_until = alone_deadline(&room);
        let ended = loop {
            let event = tokio::select! {
                event = events.recv() => event,
                _ = leave.wait_for(|leave| *leave) => break Ended::Finished,
                () = sleep_until_or_forever(alone_until) => {
                    alone_until = None;
                    // While the SDK reconnects, nobody shows: `Reconnected` checks again.
                    if room.connection_state() != ConnectionState::Connected {
                        continue;
                    }
                    info!(room = room_name, "nobody else in the room, leaving");
                    break Ended::Finished;
                }
            };
            let Some(event) = event else {
                break Ended::Dropped;
            };
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
                RoomEvent::ParticipantConnected(_) => alone_until = None,
                RoomEvent::ParticipantDisconnected(participant) => {
                    if let Some(old) = speakers.remove(&participant.identity()) {
                        old.pump.abort();
                    }
                    // While the SDK reconnects it drops every participant; people coming back
                    // then reappear, so only a departure on a live connection ends the call.
                    if room.remote_participants().is_empty() {
                        if room.connection_state() == ConnectionState::Connected {
                            break Ended::Finished;
                        }
                        alone_until = alone_deadline(&room);
                    }
                }
                RoomEvent::Reconnecting => info!(
                    room = room_name,
                    "LiveKit connection lost, the SDK is resuming it"
                ),
                RoomEvent::Reconnected => {
                    info!(room = room_name, "LiveKit connection resumed");
                    // A full reconnection recreates the room if it was gone: nobody may come.
                    alone_until = alone_deadline(&room);
                }
                RoomEvent::Disconnected { reason } => {
                    break if ends_the_room(reason) {
                        Ended::Finished
                    } else {
                        info!(room = room_name, reason = ?reason, "disconnected from LiveKit");
                        Ended::Dropped
                    };
                }
                _ => {}
            }
        };

        for speaker in speakers.into_values() {
            speaker.pump.abort();
        }
        room.close().await.ok();
        info!(room = room_name, "agent left");
        ended
    }

    /// Connects to `room` again after an outage, with a growing, bounded delay, while it exists.
    ///
    /// Each call starts counting attempts from zero, so a recovered connection gets the full
    /// budget at its next outage.
    async fn reconnect(
        &self,
        room: &str,
        leave: &mut watch::Receiver<bool>,
    ) -> Option<(Room, mpsc::UnboundedReceiver<RoomEvent>)> {
        let mut missing = 0;
        for attempt in 1..=RECONNECT_ATTEMPTS {
            tokio::select! {
                () = tokio::time::sleep(reconnect_delay(attempt)) => {}
                _ = leave.wait_for(|leave| *leave) => return None,
            }
            match self.room_exists(room).await {
                Ok(true) => {
                    missing = 0;
                    match tokio::time::timeout(JOIN_TIMEOUT, self.connect(room)).await {
                        Ok(Ok(connection)) => {
                            info!(room, attempt, "back in the room");
                            return Some(connection);
                        }
                        Ok(Err(_)) | Err(_) => info!(room, attempt, "reconnection failed"),
                    }
                }
                Ok(false) => {
                    missing += 1;
                    if missing >= ROOM_GONE_CHECKS {
                        info!(room, "the room is gone, not reconnecting");
                        return None;
                    }
                }
                Err(_) => info!(room, attempt, "LiveKit unreachable, retrying"),
            }
        }
        info!(
            room,
            attempts = RECONNECT_ATTEMPTS,
            "LiveKit still unreachable, giving up on the room"
        );
        None
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
        let locales = attributes
            .get(&self.config.lang_attribute)
            .and_then(|raw| parse_locales(raw))
            .with_context(|| {
                format!(
                    "participant has no valid `{}` attribute (one to 10 Azure locales)",
                    self.config.lang_attribute
                )
            })?;
        let target = match attributes.get(&self.config.target_attribute) {
            None => None,
            Some(raw) => match parse_target(raw) {
                Some(target) => Some(target),
                None => {
                    warn!(speaker = %identity.0, attribute = %self.config.target_attribute, "invalid target language, captions not translated");
                    None
                }
            },
        };
        let lang = locales.join(",");
        let session = new_id();
        info!(speaker = %identity.0, %lang, target = target.as_deref().unwrap_or("-"), %session, "captioning");

        let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_BUFFER);
        let (event_tx, event_rx) = mpsc::channel::<CaptionEvent>(64);

        // The connection ends when the audio channel closes (speaker gone, room left) or after
        // too many failed reconnections. Azure keeps neither the audio nor the text of a live
        // transcription, so nothing is left to delete. Only the session id is logged.
        self.sessions.increment();
        let sessions = Arc::clone(&self.sessions);
        let speech = self.speech.clone();
        tokio::spawn(async move {
            match speech
                .transcribe(&session, &locales, audio_rx, event_tx)
                .await
            {
                Ok(()) => info!(%session, "Azure Speech session closed"),
                Err(err) => {
                    warn!(%session, error = %format!("{err:#}"), "Azure Speech session ended")
                }
            }
            sessions.decrement();
        });
        tokio::spawn(publish_captions(
            room,
            identity.clone(),
            lang,
            target,
            self.translator.clone(),
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
///
/// Only finals that are not suspect reach the listeners. They are translated to `target`, unless
/// they are already in it; a failed translation sends nothing.
async fn publish_captions(
    room: Arc<Room>,
    speaker: ParticipantIdentity,
    lang: String,
    target: Option<String>,
    translator: TranslatorClient,
    mut events: mpsc::Receiver<CaptionEvent>,
) {
    while let Some(mut event) = events.recv().await {
        if event.kind == CaptionKind::Final && is_suspect(&event.text) {
            event.suspect = true;
        }
        let local = room.local_participant();
        if let Some(bytes) = encode(&event, &speaker.0, &lang) {
            let packet = DataPacket {
                payload: bytes,
                topic: Some(TOPIC_SELF.to_owned()),
                reliable: true,
                destination_identities: vec![speaker.clone()],
            };
            if let Err(err) = local.publish_data(packet).await {
                warn!(%err, "cannot send transcript to speaker");
            }
        }
        if event.kind != CaptionKind::Final || event.suspect {
            continue;
        }
        let Some(detected) = event.language.clone() else {
            continue;
        };
        let for_listeners = match target.as_deref() {
            Some(to) if !same_language(&detected, to) => {
                match translator
                    .translate(&event.text, &translator_code(&detected), to)
                    .await
                {
                    Ok(text) => CaptionEvent {
                        kind: CaptionKind::Translation,
                        original: Some(std::mem::take(&mut event.text)),
                        text,
                        ..event
                    },
                    Err(err) => {
                        warn!(error = %format!("{err:#}"), "translation failed, caption not sent");
                        continue;
                    }
                }
            }
            _ => event,
        };
        let listeners: Vec<ParticipantIdentity> = room
            .remote_participants()
            .into_keys()
            .filter(|id| *id != speaker)
            .collect();
        let Some(bytes) = encode(&for_listeners, &speaker.0, &lang) else {
            continue;
        };
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

fn encode(event: &CaptionEvent, speaker: &str, lang: &str) -> Option<Vec<u8>> {
    serde_json::to_vec(&to_payload(event, speaker, lang)).ok()
}

/// Disconnections after which there is no room to come back to.
fn ends_the_room(reason: livekit::DisconnectReason) -> bool {
    matches!(
        reason,
        livekit::DisconnectReason::ClientInitiated
            | livekit::DisconnectReason::DuplicateIdentity
            | livekit::DisconnectReason::ParticipantRemoved
            | livekit::DisconnectReason::RoomDeleted
            | livekit::DisconnectReason::RoomClosed
    )
}

/// 1, 2, 4, 8 then 15 s between reconnection attempts.
fn reconnect_delay(attempt: u32) -> Duration {
    let secs = 1u64 << attempt.saturating_sub(1).min(4);
    Duration::from_secs(secs.min(15))
}

fn alone_deadline(room: &Room) -> Option<tokio::time::Instant> {
    room.remote_participants()
        .is_empty()
        .then(|| tokio::time::Instant::now() + ALONE_TIMEOUT)
}

async fn sleep_until_or_forever(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
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

fn to_payload<'a>(event: &'a CaptionEvent, speaker: &'a str, lang: &'a str) -> CaptionPayload<'a> {
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
        suspect: event.suspect,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{http_base, reconnect_delay, to_payload};

    #[test]
    fn reconnection_delays_grow_and_stay_bounded() {
        let delays: Vec<u64> = (1..=7).map(|a| reconnect_delay(a).as_secs()).collect();
        assert_eq!(delays, [1, 2, 4, 8, 15, 15, 15]);
        assert_eq!(reconnect_delay(u32::MAX), Duration::from_secs(15));
    }

    #[test]
    fn the_server_api_lives_on_the_http_side_of_the_signalling_url() {
        assert_eq!(
            http_base("wss://x.livekit.cloud/"),
            "https://x.livekit.cloud"
        );
        assert_eq!(http_base("ws://127.0.0.1:7880"), "http://127.0.0.1:7880");
        assert_eq!(http_base("https://lk.example"), "https://lk.example");
    }
    use crate::speech::{CaptionEvent, CaptionKind};

    #[test]
    fn payload_uses_camel_case_and_skips_empty_fields() -> anyhow::Result<()> {
        let event = CaptionEvent {
            utterance_id: "u1".into(),
            kind: CaptionKind::Translation,
            text: "Je suis fatigué.".into(),
            original: Some("Vizaka aho.".into()),
            language: Some("mg-MG".into()),
            confidence: None,
            suspect: false,
        };
        let json = serde_json::to_value(to_payload(&event, "aline", "mg-MG"))?;
        assert_eq!(json["kind"], "translation");
        assert_eq!(json["detectedLang"], "mg-MG");
        assert_eq!(json["suspect"], false);
        assert_eq!(json["original"], "Vizaka aho.");
        assert!(json.get("confidence").is_none());
        Ok(())
    }
}
