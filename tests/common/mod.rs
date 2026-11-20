//! Test bench shared by the integration tests: a fake Gladia and a local LiveKit server.

#![allow(dead_code, reason = "each test binary uses a different part of the bench")]

pub mod fake_gladia;
pub mod livekit;

/// Serializes the tests that run WebRTC: one LiveKit server and one agent at a time keeps them
/// reliable on a small CI runner.
pub static LIVEKIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
