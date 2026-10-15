# Contributing to livecap

Thanks for helping. Small, focused pull requests are easiest to review.

## Setup

1. Install Rust (stable, 1.88 or later), the glib development headers (`libglib2.0-dev` on Debian/Ubuntu) and **clang 21 or later**: the LiveKit SDK links a prebuilt libwebrtc that checks the compiler version.
2. Build and test:
   ```bash
   CC=clang-21 CXX=clang++-21 cargo build
   cargo test
   cargo clippy --all-targets -- -D warnings
   cargo fmt --check
   ```
3. To try it end to end, run `livekit-server --dev` locally, set the environment variables listed in the README, and join a room from two browser tabs with tokens carrying the `livecap.lang` / `livecap.target` attributes.

## Conventions

- Commits follow [Conventional Commits](https://www.conventionalcommits.org): `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `ci:`, `chore:`.
- No `.unwrap()` or `.expect()` outside tests; errors carry context (`anyhow::Context`).
- New behaviour comes with a unit test. Parsing code (provider messages) is tested against real payload samples.
- Never commit API keys, recordings or transcripts of real people.

## Adding a speech-to-text provider

Providers live behind the session runner in `src/gladia.rs` today. A `SpeechProvider` trait is on the roadmap; open an issue first so we agree on its shape.

## Code of conduct

Be kind and assume good faith. Harassment of any kind is not tolerated.
