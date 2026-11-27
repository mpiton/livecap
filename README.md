# livecap

[![CI](https://github.com/mpiton/livecap/actions/workflows/ci.yml/badge.svg)](https://github.com/mpiton/livecap/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Live, translated captions for [LiveKit](https://livekit.io) calls, written in Rust.

The agent joins a room as a **hidden participant**, listens to each speaker's audio track, streams it to a real-time speech-to-text service (currently [Azure AI Speech](https://learn.microsoft.com/azure/ai-services/speech-service/)), translates each finished sentence with [Azure Translator](https://learn.microsoft.com/azure/ai-services/translator/), and publishes the captions back to the room as data messages. Phones only upload their voice once, to LiveKit.

LiveKit's own Agents framework exists in Python and Node.js only; this is a small, self-hostable alternative for the one job of captioning.

```
speaker ──audio──► LiveKit ──audio──► livecap ──PCM 16 kHz──► Azure AI Speech
                      ▲                 │  ▲◄──── partials + finals
                      │                 │  └──── final ──────► Azure Translator
                      │                 │◄──────── translation
listener ◄──captions──┴── data message ─┘
```

## Status

Young, but meant for production: v0.2.0 moves to Azure AI Speech (configurable region, `francecentral` by default) and Azure Translator, with several locales per speaker, suspect finals kept from listeners, a `join` that answers only once the agent is in the room, a room cap, a graceful stop and recovery from LiveKit and Azure outages. CI runs every change against a fake Azure AI Speech, a fake Azure Translator and a local `livekit-server --dev`. Tagged releases publish a Linux x86_64 binary; see [docs/deploy.md](docs/deploy.md) to run it with systemd and Caddy. Expect breaking changes before 1.0.

## How it works

1. Something tells the agent to join a room: a signed LiveKit webhook (`participant_joined`), or `POST /rooms/{room}/join` with a shared secret.
2. For every audio track it subscribes to, the agent reads two **participant attributes** set in that participant's access token:
   - `livecap.lang`: the [Azure locale](https://learn.microsoft.com/azure/ai-services/speech-service/language-support?tabs=stt) the participant speaks (e.g. `fr-FR`), or a comma-separated list of at most 10 locales they mix (e.g. `fr-FR,en-US`): with more than one, Azure detects the language continuously, among those locales only;
   - `livecap.target`: the language the *other* participant reads, as a [Translator code](https://learn.microsoft.com/azure/ai-services/translator/language-support) (`fr`, `zh-Hans`) or a locale (`fr-FR`) (optional; absent means no translation).

   An empty, invalid or too long `livecap.lang` does not stop the agent: that track gets no captions, and the log says so without the value.
3. It opens one Azure AI Speech WebSocket per speaker, in the configured region, resamples the track to 16 kHz mono PCM and streams it in 40 ms chunks.
4. It publishes, as JSON data messages:
   - on topic `livecap.self`, to the speaker only: their own live transcript (partials and finals);
   - on topic `livecap.captions`, to everyone else: each final translated by Azure Translator into `livecap.target`, or the final itself when it is already in that language (no Translator call then) or there is no target. When Translator fails, that sentence is not sent; the log says so without the text.
5. A final is flagged `suspect` and never sent to listeners nor translated when Azure did not recognize it (`RecognitionStatus` other than `Success`), when its confidence is under `AZURE_SPEECH_MIN_CONFIDENCE`, or when the detected language is not one of the speaker's locales: a language Azure does not cover, spoken by someone who declared another one, must not turn into made-up text. The speaker still sees it on `livecap.self`.
6. When Azure closes or drops the connection mid-call, the agent reconnects with a doubling delay (0.5 s up to 8 s); after 8 failures in a row it stops captioning that speaker, without affecting the room. Live transcription keeps neither the audio nor the text at Azure; livecap never turns on audio logging (`storeAudio`) and never asks Azure for diarization or speaker identification.
7. It leaves when the last participant does, or after 20 seconds alone in a room.

### Stopping the transcription of one voice

The application can stop the transcription of a participant's voice during a call, not just hide the captions: it sets the participant attribute `livecap.transcribe` (name set by `LIVECAP_TRANSCRIBE_ATTRIBUTE`) through LiveKit's server API (`RoomService.UpdateParticipant`).

- `off` (or any value other than `on`): the agent closes that voice's Azure connection at once, without waiting for the last results; no audio goes to Azure any more and nothing more is translated or sent. The listeners get `{"kind":"status","speaker":"<identity>","status":"stopped"}` on `livecap.captions`.
- `on`, or no attribute (removed with an empty value): the agent opens a new connection and the listeners get `"status":"resumed"`; captions start again.
- A participant who joins with the attribute already `off` opens no connection. `/health` counts only the open connections.

### Stopping and outages

- On **SIGTERM** (or Ctrl-C) the agent drains: `join` answers `503 {"error":"draining"}`, `/health` reports `"status":"draining"`, and the calls in progress go on until they end or `LIVECAP_DRAIN_TIMEOUT_SECS` (2 hours by default) runs out. It then leaves the remaining rooms, ends their Azure connections and exits with status 0. A second signal skips the wait. Give the service manager a longer stop timeout than the drain (see [docs/deploy.md](docs/deploy.md)).
- When the connection to LiveKit drops, the LiveKit SDK resumes it for about 45 seconds; past that the agent reconnects to the room itself, with a growing delay (1, 2, 4, 8 then 15 seconds, ten attempts), as long as the room exists. Each outage starts counting from zero. A room found gone three times in a row, or a room nobody comes back to within 20 seconds, is given up without logging errors.

Besides, short utterances that speech models typically invent on silence ("Thank you.", "Merci.", "T-Series"…) are flagged `suspect` and never sent to listeners.

### Message format

```json
{
  "id": "utterance id (partial, final and translation share it)",
  "speaker": "participant identity",
  "kind": "partial | final | translation",
  "text": "caption text",
  "original": "source text, for translations",
  "lang": "speaker locales from livecap.lang, normalised (e.g. fr-FR or fr-FR,en-US)",
  "detectedLang": "locale detected by Azure (e.g. fr-FR)",
  "confidence": 0.84,
  "suspect": false
}
```

When a speaker's transcription is switched off or back on (see above), the listeners get a status message on `livecap.captions` instead:

```json
{ "kind": "status", "speaker": "participant identity", "status": "stopped | resumed" }
```

### Setting the attributes (Node.js server SDK)

```ts
const at = new AccessToken(apiKey, apiSecret, {
  identity,
  attributes: { 'livecap.lang': 'fr-FR,en-US', 'livecap.target': 'de' },
});
```

## Running

```bash
export LIVEKIT_URL=wss://your-project.livekit.cloud
export LIVEKIT_API_KEY=...
export LIVEKIT_API_SECRET=...
export AZURE_SPEECH_KEY=...
export AZURE_TRANSLATOR_KEY=...
export AZURE_TRANSLATOR_REGION=francecentral
export LIVECAP_JOIN_SECRET=change-me   # enables POST /rooms/{room}/join

cargo run --release -- serve            # HTTP server: /webhook, /rooms/{room}/join, /health
cargo run --release -- join my-room     # or join one room and exit when it empties
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET` | required | LiveKit project |
| `AZURE_SPEECH_KEY` | required | key of the Azure AI Speech resource |
| `AZURE_SPEECH_REGION` | `francecentral` | Azure region that transcribes the audio (`wss://<region>.stt.speech.microsoft.com`) |
| `AZURE_SPEECH_URL` | from the region | base WebSocket URL, to point the agent at a fake in tests |
| `AZURE_SPEECH_MIN_CONFIDENCE` | `0.5` | 0 to 1; finals below it are `suspect` and kept from listeners (to be measured on real calls) |
| `AZURE_TRANSLATOR_KEY` | required | key of the Azure Translator resource |
| `AZURE_TRANSLATOR_REGION` | none | region of the Translator resource, sent as `Ocp-Apim-Subscription-Region` (required by regional and multi-service resources) |
| `AZURE_TRANSLATOR_URL` | `https://api.cognitive.microsofttranslator.com` | Translator endpoint; `https://api-eur.cognitive.microsofttranslator.com` keeps processing in the EU; a fake in tests |
| `LIVECAP_JOIN_SECRET` | none | bearer secret for `POST /rooms/{room}/join`; without it only webhooks work |
| `LIVECAP_MAX_ROOMS` | `15` | rooms followed at once; beyond, `join` answers `503 full`. Each speaker holds one Azure Speech connection: keep the cap within your resource's concurrency quota |
| `LIVECAP_DRAIN_TIMEOUT_SECS` | `7200` | after SIGTERM, how long calls in progress may go on before the agent leaves them |
| `LIVECAP_LISTEN_ADDR` | `0.0.0.0:8790` | HTTP address |
| `LIVECAP_AGENT_IDENTITY` | `livecap` | identity of the hidden participant |
| `LIVECAP_LANG_ATTRIBUTE`, `LIVECAP_TARGET_ATTRIBUTE` | `livecap.lang`, `livecap.target` | attribute names |
| `LIVECAP_TRANSCRIBE_ATTRIBUTE` | `livecap.transcribe` | attribute switching a voice's transcription: `on` or absent, transcribed; anything else, `off` included, not |
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
  `sessions` counts the speakers being transcribed (one Azure Speech connection each, reconnections included).
- `POST /webhook` takes LiveKit webhooks (below).

To use webhooks, point your LiveKit project's webhook URL at `https://<agent>/webhook`; requests are verified with the API key and secret.

## Why a hand-written WebSocket client

Azure has no Speech SDK for Rust. livecap speaks the WebSocket protocol of the official Speech SDK itself (`src/speech.rs`), on `/stt/speech/universal/v2`, the path the SDK uses for continuous language detection. This keeps one static binary, with no Python process or native library next to it, and makes a fake Azure easy to write for CI.

The price: the protocol has no public contract (the old "Speech WebSocket protocol" page of Microsoft Learn is gone). It is taken from the MIT source of the [Speech SDK for JavaScript](https://github.com/microsoft/cognitive-services-speech-sdk-js) at a pinned commit, cited in the code for every message, and checked against the Azure-Samples guide [Speech translation via WebSocket (no SDK)](https://github.com/Azure-Samples/voice-translator-and-personal-voice/blob/main/HOW_TO_USE_SPEECH_TRANSLATION_WEBSOCKETS.md). A canary test against the real service (below) runs before each tag.

**Fallback** if Microsoft breaks the protocol: call the official Speech SDK for C through FFI, still in one process. Not a Python or C++ SDK process next to livecap.

## Building

The LiveKit Rust SDK links a prebuilt libwebrtc that needs **clang 21 or later** (`webrtc-sys` checks it at build time) and the glib headers (`libglib2.0-dev` on Debian/Ubuntu):

```bash
CC=clang-21 CXX=clang++-21 cargo build --release
```

To deploy without compiling, use the binary attached to each [release](https://github.com/mpiton/livecap/releases) (`livecap-x86_64-unknown-linux-gnu`, with its `.sha256`) and follow [docs/deploy.md](docs/deploy.md).

## Testing

```bash
cargo test
```

runs the unit tests and the integration tests under `tests/`. None of them calls Azure or LiveKit Cloud:

- `tests/common/fake_speech.rs` is a fake Azure AI Speech: it accepts the Speech SDK WebSocket on `/stt/speech/universal/v2` (and the one-language `/speech/recognition/conversation/cognitiveservices/v1`), records the URL, the headers, `speech.config`, `speech.context` and the audio frames (RIFF header, then PCM), answers with `speech.hypothesis`, `speech.phrase` (confidence, detected language) and `turn.end` written in advance, can send a low-confidence or undeclared-language final and cut connections mid-stream, and replays what it received on `GET /calls`. The agent is pointed at it with `AZURE_SPEECH_URL`.
- `tests/common/fake_translator.rs` is a fake Azure Translator: `POST /translate?api-version=3.0` records the key and region headers, `from`, `to` and the text, and answers a translation written in advance, or an error on demand. The agent is pointed at it with `AZURE_TRANSLATOR_URL`.
- `tests/speech.rs` drives the Azure clients against those fakes and pins what they send.
- `tests/agent.rs` runs the `livecap` binary against the fakes and a local `livekit-server` in dev mode, with test participants that publish a tone and read the captions. It also checks that no transcript, no translation and no key reaches the agent's logs (captured at `RUST_LOG=debug,livekit=info,livekit_api=info,libwebrtc=info`: at `debug` the LiveKit SDK itself prints whole signalling messages, see [Logs](docs/deploy.md#logs)).

The end-to-end tests need a `livekit-server` binary (1.13 or later) named by `LIVEKIT_SERVER_BIN`; without it they are **skipped** and print `SKIPPED <test>: set LIVEKIT_SERVER_BIN ...`, except on CI (`CI` set), where a missing binary fails them. CI downloads a pinned release and checks its SHA-256. Locally:

```bash
curl -fsSLO https://github.com/livekit/livekit/releases/download/v1.13.11/livekit_1.13.11_linux_amd64.tar.gz
echo "1e9ee90ca301e1842b2887fc77cb122759756a9c8aeb4334ee91de3325ec43fb  livekit_1.13.11_linux_amd64.tar.gz" | sha256sum -c
tar xzf livekit_1.13.11_linux_amd64.tar.gz livekit-server
LIVEKIT_SERVER_BIN=$PWD/livekit-server cargo test
```

Each test starts its own `livekit-server` and agent on ports the kernel hands out (the ephemeral range), so nothing needs to be free beforehand; the tests that use WebRTC run one at a time.

### Canary against the real Azure

`tests/canary.rs` sends a French sentence (`tests/fixtures/canary-fr.wav`) to the real Azure AI Speech and a sentence to the real Azure Translator. It is ignored by default and never runs on CI; run it before each tag, with the keys of a test resource:

```bash
AZURE_SPEECH_KEY=... AZURE_SPEECH_REGION=francecentral \
AZURE_TRANSLATOR_KEY=... AZURE_TRANSLATOR_REGION=francecentral \
  cargo test --test canary -- --ignored
```

A failure there while the fakes pass means Microsoft changed the protocol: see the fallback above.

## Costs, for reference

Azure AI Speech, real-time speech to text (S0, November 2026): about €0.90 per hour of audio per speaker. Azure Translator: $10 per million characters, about 900 characters per minute of conversation. The agent counts as one more LiveKit participant.

## Roadmap

- A `SpeechProvider` trait, with a second backend.
- Per-listener target languages for rooms with more than two people.
- A Dockerfile, for those who deploy containers rather than the systemd setup of [docs/deploy.md](docs/deploy.md).

## License

MIT
