//! A `livekit-server --dev` child process, the agent binary as a child process, and test
//! participants that publish a tone or collect captions.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use livekit::options::TrackPublishOptions;
use livekit::prelude::*;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::webrtc::audio_source::{AudioSourceOptions, RtcAudioSource};
use livekit::webrtc::prelude::AudioFrame;
use livekit_api::access_token::{AccessToken, VideoGrants};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// API key and secret of `livekit-server --dev`.
pub const API_KEY: &str = "devkey";
pub const API_SECRET: &str = "secret";
/// `RUST_LOG` the agent runs with in tests.
pub const LOG_FILTER: &str = "debug,livekit=info,livekit_api=info,libwebrtc=info";
/// Bearer secret the agent is started with.
pub const JOIN_SECRET: &str = "test-join-secret";

/// Path of the `livekit-server` binary, from `LIVEKIT_SERVER_BIN`.
pub fn livekit_server_bin() -> Option<PathBuf> {
    std::env::var_os("LIVEKIT_SERVER_BIN")
        .map(PathBuf::from)
        .filter(|p| p.is_file())
}

/// Returns the binary, or prints why the calling test is skipped.
///
/// On CI (`CI` set, as GitHub Actions does) a missing binary fails the test instead: the
/// end-to-end tests must not pass there by skipping.
pub fn require_livekit(test: &str) -> Option<PathBuf> {
    let bin = livekit_server_bin();
    assert!(
        bin.is_some() || std::env::var_os("CI").is_none(),
        "{test}: LIVEKIT_SERVER_BIN must name a livekit-server binary on CI"
    );
    if bin.is_none() {
        eprintln!(
            "SKIPPED {test}: set LIVEKIT_SERVER_BIN to a livekit-server binary to run this test"
        );
    }
    bin
}

/// A port the kernel just handed out (ephemeral range, so never one of the fixed ports other
/// projects use on this machine), released before use.
pub fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap()
}

fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .and_then(|s| s.local_addr())
        .map(|a| a.port())
        .unwrap()
}

pub struct LiveKitServer {
    bin: PathBuf,
    child: Option<Child>,
    pub port: u16,
    tcp_port: u16,
    udp_port: u16,
}

impl LiveKitServer {
    pub async fn start(bin: PathBuf) -> Self {
        let mut server = Self {
            bin,
            child: None,
            port: free_tcp_port(),
            tcp_port: free_tcp_port(),
            udp_port: free_udp_port(),
        };
        server.spawn().await;
        server
    }

    pub fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }

    async fn spawn(&mut self) {
        let config = format!(
            "port: {}\nrtc:\n  tcp_port: {}\n  udp_port: {}\n  use_external_ip: false\nlogging:\n  level: warn\n",
            self.port, self.tcp_port, self.udp_port
        );
        let child = Command::new(&self.bin)
            .args(["--dev", "--bind", "127.0.0.1", "--node-ip", "127.0.0.1"])
            .args(["--config-body", &config])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        self.child = Some(child);
        let url = format!("http://127.0.0.1:{}/", self.port);
        wait_until(Duration::from_secs(15), || async {
            reqwest::get(&url)
                .await
                .is_ok_and(|r| r.status().is_success())
        })
        .await;
    }

    /// Kills the server (a crash, not a clean stop) and waits until it is gone.
    pub async fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().await.unwrap();
        }
    }

    /// Starts the server again on the same ports.
    pub async fn restart(&mut self) {
        self.spawn().await;
    }

    /// Identities of the participants in `room`, hidden ones included.
    pub async fn participants(&self, room: &str) -> Vec<String> {
        let body = self
            .twirp(
                "ListParticipants",
                serde_json::json!({ "room": room }),
                VideoGrants {
                    room_admin: true,
                    room: room.to_owned(),
                    ..Default::default()
                },
            )
            .await;
        body["participants"]
            .as_array()
            .map(|list| {
                list.iter()
                    .filter_map(|p| p["identity"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn twirp(&self, method: &str, body: Value, grants: VideoGrants) -> Value {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_grants(grants)
            .to_jwt()
            .unwrap();
        reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/twirp/livekit.RoomService/{method}",
                self.port
            ))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub async fn room_names(&self) -> Vec<String> {
        let token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_grants(VideoGrants {
                room_list: true,
                ..Default::default()
            })
            .to_jwt()
            .unwrap();
        let body: Value = reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/twirp/livekit.RoomService/ListRooms",
                self.port
            ))
            .bearer_auth(token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        body["rooms"]
            .as_array()
            .map(|rooms| {
                rooms
                    .iter()
                    .filter_map(|r| r["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The `livecap` binary, run as a child process with its output captured.
pub struct AgentProcess {
    child: Child,
    pub base_url: String,
    logs: Arc<Mutex<String>>,
}

impl AgentProcess {
    /// Starts `livecap serve` against `livekit_url` and `gladia_url`, plus `extra` variables.
    pub async fn start(livekit_url: &str, gladia_url: &str, extra: &[(&str, &str)]) -> Self {
        let port = free_tcp_port();
        let mut command = Command::new(env!("CARGO_BIN_EXE_livecap"));
        command
            .arg("serve")
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("LIVEKIT_URL", livekit_url)
            .env("LIVEKIT_API_KEY", API_KEY)
            .env("LIVEKIT_API_SECRET", API_SECRET)
            .env("GLADIA_API_KEY", "test-gladia-key")
            .env("GLADIA_API_URL", gladia_url)
            .env("LIVECAP_JOIN_SECRET", JOIN_SECRET)
            .env("LIVECAP_LISTEN_ADDR", format!("127.0.0.1:{port}"))
            // Everything the agent logs, at the most verbose level an operator would use. The
            // LiveKit SDK stays at `info`: at `debug` it prints whole signalling messages
            // (participant attributes, TURN credentials), which the deployment guide warns about.
            .env("RUST_LOG", LOG_FILTER)
            .envs(extra.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap();
        let logs = Arc::new(Mutex::new(String::new()));
        if let Some(out) = child.stdout.take() {
            collect(out, Arc::clone(&logs));
        }
        if let Some(err) = child.stderr.take() {
            collect(err, Arc::clone(&logs));
        }
        let agent = Self {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
            logs,
        };
        let health = format!("{}/health", agent.base_url);
        wait_until(Duration::from_secs(15), || async {
            reqwest::get(&health).await.is_ok()
        })
        .await;
        agent
    }

    pub fn logs(&self) -> String {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// `POST /rooms/{room}/join` with the right secret: status and body.
    pub async fn join(&self, room: &str) -> (u16, String) {
        let response = reqwest::Client::new()
            .post(format!("{}/rooms/{room}/join", self.base_url))
            .bearer_auth(JOIN_SECRET)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.text().await.unwrap_or_default())
    }

    pub async fn health(&self) -> Value {
        reqwest::get(format!("{}/health", self.base_url))
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub fn sigterm(&self) {
        let pid = self.child.id().unwrap().to_string();
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Waits for the process to exit, or returns `None` after `timeout`.
    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        tokio::time::timeout(timeout, self.child.wait())
            .await
            .ok()
            .and_then(Result::ok)
    }
}

fn collect<R: tokio::io::AsyncRead + Unpin + Send + 'static>(reader: R, logs: Arc<Mutex<String>>) {
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut logs = logs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            logs.push_str(&line);
            logs.push('\n');
        }
    });
}

pub fn token(identity: &str, room: &str, attributes: &[(&str, &str)]) -> String {
    AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity(identity)
        .with_attributes(attributes.iter().map(|(k, v)| (*k, *v)))
        .with_grants(VideoGrants {
            room_join: true,
            room: room.to_owned(),
            can_publish: Some(true),
            can_subscribe: Some(true),
            can_publish_data: Some(true),
            ..Default::default()
        })
        .to_jwt()
        .unwrap()
}

/// A participant publishing a 440 Hz tone on its microphone track.
pub struct Speaker {
    pub room: Room,
    tone: JoinHandle<()>,
}

impl Speaker {
    pub async fn join(url: &str, room: &str, identity: &str, attributes: &[(&str, &str)]) -> Self {
        let (lk_room, _events) = Room::connect(
            url,
            &token(identity, room, attributes),
            RoomOptions::default(),
        )
        .await
        .unwrap();
        let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1000);
        let track =
            LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
        lk_room
            .local_participant()
            .publish_track(
                LocalTrack::Audio(track),
                TrackPublishOptions {
                    source: TrackSource::Microphone,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let tone = tokio::spawn(async move {
            let mut phase = 0f32;
            loop {
                let samples: Vec<i16> = (0..480)
                    .map(|_| {
                        phase += 440.0 * std::f32::consts::TAU / 48_000.0;
                        (phase.sin() * 8000.0) as i16
                    })
                    .collect();
                let frame = AudioFrame {
                    data: samples.into(),
                    sample_rate: 48_000,
                    num_channels: 1,
                    samples_per_channel: 480,
                };
                if source.capture_frame(&frame).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });
        Self {
            room: lk_room,
            tone,
        }
    }

    pub async fn leave(self) {
        self.tone.abort();
        self.room.close().await.ok();
    }
}

/// A participant collecting what arrives on a data topic.
pub struct Listener {
    pub room: Room,
    pub captions: mpsc::UnboundedReceiver<Value>,
}

impl Listener {
    pub async fn join(url: &str, room: &str, identity: &str, topic: &'static str) -> Self {
        let (lk_room, mut events) =
            Room::connect(url, &token(identity, room, &[]), RoomOptions::default())
                .await
                .unwrap();
        let (tx, captions) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if let RoomEvent::DataReceived {
                    payload,
                    topic: Some(t),
                    ..
                } = event
                    && t == topic
                    && let Ok(value) = serde_json::from_slice::<Value>(&payload)
                    && tx.send(value).is_err()
                {
                    return;
                }
            }
        });
        Self {
            room: lk_room,
            captions,
        }
    }

    /// Next caption, or `None` after `timeout`.
    pub async fn next(&mut self, timeout: Duration) -> Option<Value> {
        tokio::time::timeout(timeout, self.captions.recv())
            .await
            .ok()
            .flatten()
    }

    /// Drops what already arrived.
    pub fn clear(&mut self) {
        while self.captions.try_recv().is_ok() {}
    }

    pub async fn leave(self) {
        self.room.close().await.ok();
    }
}

/// Polls `check` until it is true; panics after `timeout`.
pub async fn wait_until<F, Fut>(timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
