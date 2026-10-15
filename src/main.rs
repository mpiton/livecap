//! Live translated captions for LiveKit calls.
//!
//! `livecap serve` waits for LiveKit webhooks (or `POST /rooms/{room}/join`) and joins
//! rooms as a hidden participant. `livecap join <room>` joins one room and exits when it empties.

mod audio;
mod config;
mod filter;
mod gladia;
mod room;
mod server;

use anyhow::{Context, Result, bail};
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::room::Agent;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Several TLS backends are linked in (LiveKit, Gladia, HTTP): pick one explicitly.
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        bail!("cannot install the TLS crypto provider");
    }

    let config = Config::from_env()?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None | Some("serve") => serve(config).await,
        Some("join") => {
            let room = args.next().context("usage: livecap join <room>")?;
            if !server::valid_room_name(&room) {
                bail!("invalid room name");
            }
            Agent::new(config).run(&room).await
        }
        Some(other) => bail!("unknown command `{other}` (expected `serve` or `join <room>`)"),
    }
}

async fn serve(config: Config) -> Result<()> {
    let addr = config.listen_addr.clone();
    let app = server::router(
        Agent::new(config.clone()),
        &config.livekit_api_key,
        &config.livekit_api_secret,
        config.join_secret.clone(),
    );
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot listen on {addr}"))?;
    tracing::info!(%addr, "captions agent ready");
    axum::serve(listener, app).await.context("server stopped")
}
