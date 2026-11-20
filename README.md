# livecap

[![CI](https://github.com/mpiton/livecap/actions/workflows/ci.yml/badge.svg)](https://github.com/mpiton/livecap/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Live, translated captions for [LiveKit](https://livekit.io) calls, written in Rust.

The agent joins a room as a **hidden participant**, listens to each speaker's audio track, streams it to a real-time speech-to-text service (currently [Gladia](https://www.gladia.io), which can also translate), and publishes the captions back to the room as data messages. Phones only upload their voice once, to LiveKit.

LiveKit's own Agents framework exists in Python and Node.js only; this is a small, self-hostable alternative for the one job of captioning.

```
speaker ──audio──► LiveKit ──audio──► livecap ──PCM 16 kHz──► Gladia
                      ▲                     │◄──── transcript + translation
listener ◄──captions──┴──── data message ───┘
```

## Status

Early, working prototype. Tested end to end against a local `livekit-server --dev`, two browsers and a fake Gladia server; first real-world tests France ↔ Madagascar are next. Expect breaking changes.

## How it works

1. Something tells the agent to join a room: a signed LiveKit webhook (`participant_joined`), or `POST /rooms/{room}/join` with a shared secret.
2. For every audio track it subscribes to, the agent reads two **participant attributes** set in that participant's access token:
   - `livecap.lang`: the language the participant speaks (ISO 639-1, e.g. `fr`), or a comma-separated list of the languages they mix (e.g. `fr,mg`): with more than one, Gladia's code switching is turned on, restricted to those languages;
   - `livecap.target`: the language the *other* participant reads (optional; absent, or the only language spoken, means no translation).

   An empty or invalid `livecap.lang` (codes are two or three letters) does not stop the agent: that track gets no captions, and the log says so without the value.
3. It opens one Gladia live session per speaker, in the configured region, resamples the track to 16 kHz mono PCM and streams it in 40 ms chunks.
4. It publishes, as JSON data messages:
   - on topic `livecap.self`, to the speaker only: their own live transcript (partials and finals);
   - on topic `livecap.captions`, to everyone else: the translation (or the final transcript when not translating).
5. When a speaker leaves (or their track ends, the room empties, the agent stops, or Gladia fails the session), the agent closes their Gladia session and deletes it (`DELETE /v2/live/{id}`), so Gladia keeps neither the audio nor the transcript. A refused deletion is retried with a doubling delay for about 15 seconds; only the session id is logged.
6. It leaves when the last participant does, or after 20 seconds alone in a room.

### Stopping and outages

- On **SIGTERM** (or Ctrl-C) the agent drains: `join` answers `503 {"error":"draining"}`, `/health` reports `"status":"draining"`, and the calls in progress go on until they end or `LIVECAP_DRAIN_TIMEOUT_SECS` (2 hours by default) runs out. It then leaves the remaining rooms, deletes their Gladia sessions and exits with status 0. A second signal skips the wait. Give the service manager a longer stop timeout than the drain (see [docs/deploy.md](docs/deploy.md)).
- When the connection to LiveKit drops, the LiveKit SDK resumes it for about 45 seconds; past that the agent reconnects to the room itself, with a growing delay (1, 2, 4, 8 then 15 seconds, ten attempts), as long as the room exists. Each outage starts counting from zero. A room found gone three times in a row, or a room nobody comes back to within 20 seconds, is given up without logging errors.

Short utterances that speech models typically invent on silence ("Thank you.", "Merci.", "T-Series"…) are flagged `suspect` and never sent to listeners.

### Message format

```json
{
  "id": "utterance id (partial, final and translation share it)",
  "speaker": "participant identity",
  "kind": "partial | final | translation",
  "text": "caption text",
  "original": "source text, for translations",
  "lang": "speaker languages from livecap.lang, normalised (e.g. fr or fr,mg)",
  "detectedLang": "language detected by the model",
  "confidence": 0.84,
  "suspect": false
}
```

### Setting the attributes (Node.js server SDK)

```ts
const at = new AccessToken(apiKey, apiSecret, {
  identity,
  attributes: { 'livecap.lang': 'mg,fr', 'livecap.target': 'fr' },
});
```

## Running

```bash
export LIVEKIT_URL=wss://your-project.livekit.cloud
export LIVEKIT_API_KEY=...
export LIVEKIT_API_SECRET=...
export GLADIA_API_KEY=...
export LIVECAP_JOIN_SECRET=change-me   # enables POST /rooms/{room}/join

cargo run --release -- serve            # HTTP server: /webhook, /rooms/{room}/join, /health
cargo run --release -- join my-room     # or join one room and exit when it empties
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` | required | LiveKit project |
| `GLADIA_API_KEY` | required | Gladia key |
| `GLADIA_API_URL` | `https://api.gladia.io` | override for tests |
| `GLADIA_REGION` | `eu-west` | Gladia region that processes the audio (`eu-west` or `us-west`), sent on every session opening |
| `GLADIA_SPEECH_THRESHOLD` | `0.75` | 0 to 1; higher ignores more background noise |
| `LIVECAP_JOIN_SECRET` | none | bearer secret for `POST /rooms/{room}/join`; without it only webhooks work |
| `LIVECAP_MAX_ROOMS` | `15` | rooms followed at once; beyond, `join` answers `503 full`. Each speaker holds one Gladia session: 15 two-person rooms stay under the 30 concurrent sessions of a paid Gladia account |
| `LIVECAP_DRAIN_TIMEOUT_SECS` | `7200` | after SIGTERM, how long calls in progress may go on before the agent leaves them |
| `LIVECAP_LISTEN_ADDR` | `0.0.0.0:8790` | HTTP address |
| `LIVECAP_AGENT_IDENTITY` | `livecap` | identity of the hidden participant |
| `LIVECAP_LANG_ATTRIBUTE`, `LIVECAP_TARGET_ATTRIBUTE` | `livecap.lang`, `livecap.target` | attribute names |
| `RUST_LOG` | `info` | log filter |

### HTTP API

- `POST /rooms/{room}/join` with `Authorization: Bearer $LIVECAP_JOIN_SECRET` answers once it has tried, within about 10 seconds:
  - `200 {"status":"joined"}` when the agent is in the room (a room already followed answers 200 too, without a second agent);
  - `503 {"error":"full"}` when `LIVECAP_MAX_ROOMS` rooms are already followed;
  - `503 {"error":"join-failed"}` when the room does not exist (the agent never creates one), or LiveKit refused or did not answer in time;
  - `503 {"error":"draining"}` once the agent is stopping;
  - `401` without the right secret, `400` for a room name outside `[A-Za-z0-9._-]{1,128}`, `404` when no secret is configured.
- `GET /health` answers `200` with the version and the load, never a room name:

  ```json
  { "status": "ok", "version": "0.2.0", "rooms": 1, "sessions": 2, "maxRooms": 15 }
  ```

  `status` is `draining` once the agent is stopping.
  `sessions` counts the Gladia sessions opened and not yet deleted.
- `POST /webhook` takes LiveKit webhooks (below).

To use webhooks, point your LiveKit project's webhook URL at `https://<agent>/webhook`; requests are verified with the API key and secret.

## Building

The LiveKit Rust SDK links a prebuilt libwebrtc that needs **clang 21 or later** (`webrtc-sys` checks it at build time) and the glib headers (`libglib2.0-dev` on Debian/Ubuntu):

```bash
CC=clang-21 CXX=clang++-21 cargo build --release
```

## Testing

```bash
cargo test
```

runs the unit tests and the integration tests under `tests/`. None of them calls Gladia or LiveKit Cloud:

- `tests/common/fake_gladia.rs` is a fake Gladia live API (`POST /v2/live`, the session WebSocket, `DELETE /v2/live/{id}`, and `GET /calls` to replay what it received). It answers every session with captions written in advance; the agent is pointed at it with `GLADIA_API_URL`.
- `tests/agent.rs` runs the `livecap` binary against that fake and a local `livekit-server` in dev mode, with test participants that publish a tone and read the captions. It also checks that no transcript or translation text reaches the agent's logs (captured at `RUST_LOG=debug,livekit=info,livekit_api=info,libwebrtc=info`: at `debug` the LiveKit SDK itself prints whole signalling messages, see [Logs](docs/deploy.md#logs)).

The end-to-end tests need a `livekit-server` binary (1.13 or later) named by `LIVEKIT_SERVER_BIN`; without it they are **skipped** and print `SKIPPED <test>: set LIVEKIT_SERVER_BIN ...`. CI downloads a pinned release and checks its SHA-256. Locally:

```bash
curl -fsSLO https://github.com/livekit/livekit/releases/download/v1.13.11/livekit_1.13.11_linux_amd64.tar.gz
echo "1e9ee90ca301e1842b2887fc77cb122759756a9c8aeb4334ee91de3325ec43fb  livekit_1.13.11_linux_amd64.tar.gz" | sha256sum -c
tar xzf livekit_1.13.11_linux_amd64.tar.gz livekit-server
LIVEKIT_SERVER_BIN=$PWD/livekit-server cargo test
```

Each test starts its own `livekit-server` and agent on ports the kernel hands out (the ephemeral range), so nothing needs to be free beforehand; the tests that use WebRTC run one at a time.

## Costs, for reference

With Gladia's Starter plan (October 2026): €0.68 per hour of audio plus €0.05 per hour for translation, per speaker. The agent counts as one more LiveKit participant.

## Roadmap

- A `SpeechProvider` trait, with Deepgram as a second backend.
- Dockerfile and a deployment example.
- Per-listener target languages for rooms with more than two people.

## License

MIT
