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
   - `livecap.lang`: the language the participant speaks (ISO 639-1, e.g. `fr`);
   - `livecap.target`: the language the *other* participant reads (optional; same as `lang` or absent means no translation).
3. It opens one Gladia live session per speaker, resamples the track to 16 kHz mono PCM and streams it in 40 ms chunks.
4. It publishes, as JSON data messages:
   - on topic `livecap.self`, to the speaker only: their own live transcript (partials and finals);
   - on topic `livecap.captions`, to everyone else: the translation (or the final transcript when not translating).
5. It leaves when the last participant does.

Short utterances that speech models typically invent on silence ("Thank you.", "Merci.", "T-Series"…) are flagged `suspect` and never sent to listeners.

### Message format

```json
{
  "id": "utterance id (partial, final and translation share it)",
  "speaker": "participant identity",
  "kind": "partial | final | translation",
  "text": "caption text",
  "original": "source text, for translations",
  "lang": "speaker language from livecap.lang",
  "detectedLang": "language detected by the model",
  "confidence": 0.84,
  "suspect": false
}
```

### Setting the attributes (Node.js server SDK)

```ts
const at = new AccessToken(apiKey, apiSecret, {
  identity,
  attributes: { 'livecap.lang': 'mg', 'livecap.target': 'fr' },
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
| `GLADIA_SPEECH_THRESHOLD` | `0.75` | 0 to 1; higher ignores more background noise |
| `LIVECAP_JOIN_SECRET` | none | bearer secret for `POST /rooms/{room}/join`; without it only webhooks work |
| `LIVECAP_LISTEN_ADDR` | `0.0.0.0:8790` | HTTP address |
| `LIVECAP_AGENT_IDENTITY` | `livecap` | identity of the hidden participant |
| `LIVECAP_LANG_ATTRIBUTE`, `LIVECAP_TARGET_ATTRIBUTE` | `livecap.lang`, `livecap.target` | attribute names |
| `RUST_LOG` | `info` | log filter |

To use webhooks, point your LiveKit project's webhook URL at `https://<agent>/webhook`; requests are verified with the API key and secret.

## Building

The LiveKit Rust SDK links a prebuilt libwebrtc that needs **clang 21 or later** (`webrtc-sys` checks it at build time):

```bash
CC=clang-21 CXX=clang++-21 cargo build --release
```

## Costs, for reference

With Gladia's Starter plan (October 2026): €0.68 per hour of audio plus €0.05 per hour for translation, per speaker. The agent counts as one more LiveKit participant.

## Roadmap

- A `SpeechProvider` trait, with Deepgram as a second backend.
- Dockerfile and a deployment example.
- Per-listener target languages for rooms with more than two people.

## License

MIT
