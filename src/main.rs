//! Live translated captions for LiveKit calls.
//!
//! `livecap serve` waits for LiveKit webhooks (or `POST /rooms/{room}/join`) and joins
//! rooms as a hidden participant. `livecap join <room>` joins one room and exits when it empties.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use livecap::config::Config;
use livecap::room::Agent;
use livecap::server;
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::EnvFilter;

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
            let agent = Agent::new(config);
            agent
                .join(&room)
                .await
                .with_context(|| format!("cannot join {room}"))?;
            tokio::select! {
                () = agent.idle() => return Ok(()),
                signal = stop_signal() => signal?,
            }
            agent.shutdown().await;
            Ok(())
        }
        Some(other) => bail!("unknown command `{other}` (expected `serve` or `join <room>`)"),
    }
}

async fn serve(config: Config) -> Result<()> {
    let addr = config.listen_addr.clone();
    let agent = Agent::new(config.clone());
    let app = server::router(
        Arc::clone(&agent),
        &config.livekit_api_key,
        &config.livekit_api_secret,
        config.join_secret.clone(),
    );
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot listen on {addr}"))?;
    tracing::info!(%addr, "captions agent ready");
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    tokio::select! {
        result = server => {
            result.context("server task failed")?.context("server stopped")?;
            bail!("server stopped");
        }
        signal = stop_signal() => signal?,
    }
    tracing::info!("stop requested, leaving rooms");
    agent.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

/// Resolves on SIGTERM (systemd stop) or SIGINT (Ctrl-C).
async fn stop_signal() -> Result<()> {
    let mut terminate = signal(SignalKind::terminate()).context("cannot listen for SIGTERM")?;
    tokio::select! {
        _ = terminate.recv() => {}
        result = tokio::signal::ctrl_c() => result.context("cannot listen for SIGINT")?,
    }
    Ok(())
}
