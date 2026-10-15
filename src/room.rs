//! Joins a LiveKit room as a hidden agent and captions every speaker.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use livekit::prelude::*;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use livekit_api::access_token::{AccessToken, VideoGrants};
use serde::Serialize;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::audio::{Chunker, SAMPLE_RATE};
use crate::config::Config;
use crate::filter::is_suspect;
use crate::gladia::{CaptionEvent, CaptionKind, GladiaClient, run_session};

/// Data topic carrying captions to the people listening to a speaker.
pub const TOPIC_CAPTIONS: &str = "livecap.captions";
/// Data topic carrying a speaker's own live transcript back to them.
pub const TOPIC_SELF: &str = "livecap.self";

/// Audio chunks buffered per speaker before older ones are dropped (about 4 s).
const AUDIO_BUFFER: usize = 100;

pub struct Agent {
    config: Config,
    gladia: GladiaClient,
    active_rooms: Mutex<HashSet<String>>,
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
            config.speech_threshold,
        );
        Arc::new(Self {
            config,
            gladia,
            active_rooms: Mutex::new(HashSet::new()),
        })
    }

    pub fn identity(&self) -> &str {
        &self.config.agent_identity
    }

    /// Starts captioning `room` in the background; does nothing if the agent is already there.
    pub async fn join(self: &Arc<Self>, room: String) {
        if !self.active_rooms.lock().await.insert(room.clone()) {
            return;
        }
        let agent = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(err) = agent.run(&room).await {
                warn!(%room, error = %format!("{err:#}"), "agent stopped");
            }
            agent.active_rooms.lock().await.remove(&room);
        });
    }

    /// Stays in the room until the last person leaves.
    pub async fn run(&self, room_name: &str) -> Result<()> {
        let token = AccessToken::with_api_key(
            &self.config.livekit_api_key,
            &self.config.livekit_api_secret,
        )
        .with_identity(&self.config.agent_identity)
        .with_name("Captions")
        .with_ttl(Duration::from_secs(6 * 3600))
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.to_owned(),
            can_publish: Some(false),
            can_subscribe: Some(true),
            can_publish_data: Some(true),
            hidden: true,
            agent: true,
            ..Default::default()
        })
        .to_jwt()
        .context("cannot sign the agent token")?;

        let (room, mut events) =
            Room::connect(&self.config.livekit_url, &token, RoomOptions::default())
                .await
                .context("cannot join the LiveKit room")?;
        let room = Arc::new(room);
        info!(room = room_name, "agent joined");

        let mut speakers: HashMap<ParticipantIdentity, Speaker> = HashMap::new();
        while let Some(event) = events.recv().await {
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
        Ok(())
    }

    async fn start_speaker(
        &self,
        room: Arc<Room>,
        participant: &RemoteParticipant,
        track: RemoteAudioTrack,
    ) -> Result<Speaker> {
        let identity = participant.identity();
        let attributes = participant.attributes();
        let lang = attributes
            .get(&self.config.lang_attribute)
            .cloned()
            .with_context(|| {
                format!(
                    "participant has no `{}` attribute",
                    self.config.lang_attribute
                )
            })?;
        let target = attributes.get(&self.config.target_attribute).cloned();

        let session = self.gladia.open(&lang, target.as_deref()).await?;
        info!(speaker = %identity.0, %lang, target = target.as_deref().unwrap_or("-"), session = %session.id, "captioning");
        let translate = session.translate;

        let (audio_tx, audio_rx) = mpsc::channel::<Vec<u8>>(AUDIO_BUFFER);
        let (event_tx, event_rx) = mpsc::channel::<CaptionEvent>(64);

        let session_id = session.id.clone();
        tokio::spawn(async move {
            if let Err(err) = run_session(session, audio_rx, event_tx).await {
                warn!(session = %session_id, error = %format!("{err:#}"), "Gladia session ended");
            }
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
    use super::to_payload;
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
