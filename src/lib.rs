//! Live translated captions for LiveKit calls.
//!
//! The binary (`src/main.rs`) wires these modules together; they are exposed as a library so the
//! integration tests under `tests/` can drive the Azure clients and the HTTP API directly.

pub mod audio;
pub mod config;
pub mod filter;
pub mod room;
pub mod server;
pub mod speech;
pub mod translator;
