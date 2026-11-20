# Deploying livecap

This guide runs livecap on one Linux server for two environments (here `preprod` and `production`), each as its own systemd service under its own user, with its own LiveKit and Gladia keys, behind [Caddy](https://caddyserver.com) for TLS. Releases are prebuilt binaries: nothing is compiled on the server.

The files referred to live in [`deploy/`](../deploy):

| File | Goes to |
| --- | --- |
| [`livecap@.service`](../deploy/livecap@.service) | `/etc/systemd/system/livecap@.service` |
| [`livecap.env.example`](../deploy/livecap.env.example) | `/etc/livecap/<environment>.env` (one per environment) |
| [`Caddyfile`](../deploy/Caddyfile) | `/etc/caddy/Caddyfile` |
| [`update.sh`](../deploy/update.sh) | anywhere root can run it, e.g. `/usr/local/sbin/livecap-update` |

## Server

- Linux x86_64 with glibc 2.39 or later (Ubuntu 24.04, Debian 13 or newer): the release binary is built on Ubuntu 24.04. One small VPS (1 vCPU, 2 GB) is enough for a few rooms; livecap resamples audio and relays text, the speech-to-text runs at Gladia.
- Outbound: HTTPS and WebSocket to LiveKit and to Gladia; WebRTC (UDP, with TCP and TURN fallbacks) to LiveKit.
- Inbound: 80 and 443 for Caddy only. livecap itself listens on the loopback interface.

## Users and directories

One system user per environment, without a shell or a home:

```bash
for env in preprod production; do
  sudo useradd --system --user-group --no-create-home --shell /usr/sbin/nologin "livecap-$env"
  sudo install -d -m 0755 "/opt/livecap/$env/releases"
done
sudo install -d -m 0755 /etc/livecap
```

Binaries are owned by root (mode 0755): the service users run them but cannot replace them.

## Configuration

One environment file per environment, owned by root and readable by root only (systemd reads it before dropping privileges):

```bash
sudo install -m 0600 -o root -g root deploy/livecap.env.example /etc/livecap/preprod.env
sudo install -m 0600 -o root -g root deploy/livecap.env.example /etc/livecap/production.env
sudoedit /etc/livecap/preprod.env /etc/livecap/production.env
```

Fill in for each environment its own LiveKit project (`LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`), its own Gladia key (`GLADIA_API_KEY`), a random `LIVECAP_JOIN_SECRET` (`openssl rand -hex 32`) shared with the application that calls `join`, and a distinct loopback port: `LIVECAP_LISTEN_ADDR=127.0.0.1:8791` for `preprod`, `127.0.0.1:8792` for `production`. Every variable is described in the [README](../README.md#running). The Gladia key lives in these files only.

## systemd

```bash
sudo install -m 0644 deploy/livecap@.service /etc/systemd/system/livecap@.service
sudo systemctl daemon-reload
```

The template unit gives each instance `livecap@<environment>`:

- `User=livecap-<environment>`, `EnvironmentFile=/etc/livecap/<environment>.env`, `ExecStart=/opt/livecap/<environment>/current/livecap serve`;
- `Restart=always`, so a crash or an unexpected exit restarts it after two seconds;
- `KillSignal=SIGTERM` and `TimeoutStopSec=2h 5min`. On SIGTERM livecap drains: it answers `503 draining` to new joins, lets the calls in progress end, for at most `LIVECAP_DRAIN_TIMEOUT_SECS` (two hours by default), deletes their Gladia sessions, and exits. The stop timeout must stay above the drain timeout plus about 30 seconds, or systemd kills calls in progress. To stop at once anyway, send a second signal: `sudo systemctl kill --signal=SIGTERM livecap@preprod`;
- a sandbox (`ProtectSystem=strict`, no capabilities, no new privileges...): livecap only needs the network.

Install the first release (see [Updating](#updating)), then enable both instances:

```bash
sudo /usr/local/sbin/livecap-update preprod v0.2.0
sudo /usr/local/sbin/livecap-update production v0.2.0
sudo systemctl enable --now livecap@preprod livecap@production
curl -s http://127.0.0.1:8791/health
```

## Caddy

Caddy obtains and renews the certificates and forwards each address to its instance. Edit the two addresses in [`deploy/Caddyfile`](../deploy/Caddyfile), point their DNS records at the server, then:

```bash
sudo install -m 0644 deploy/Caddyfile /etc/caddy/Caddyfile
sudo systemctl reload caddy
curl -s https://captions-preprod.example.org/health
```

The Caddyfile has no `log` directive on purpose: access logs would record room names (they are in `/rooms/{room}/join`) and client addresses. `join` answers within about 10 seconds, well under Caddy's proxy timeouts.

The application calls `POST https://<address>/rooms/{room}/join` with `Authorization: Bearer <LIVECAP_JOIN_SECRET>`, and can watch `GET https://<address>/health` (version, load, `draining` during an update).

## Logs

livecap logs to the journal (`journalctl -u livecap@production`). At the default `RUST_LOG=info` it logs room names, participant identities, Gladia session ids, language codes, counts and errors. It never logs what people say: no transcript, no translation, no attribute value it refuses. An integration test (`no_transcript_or_translation_text_reaches_the_logs`) runs a call and searches the captured logs for the caption text.

- Keep `RUST_LOG=info`. At `debug`, the LiveKit SDK prints whole signalling messages, including participant attributes and TURN credentials. To debug livecap itself, use `RUST_LOG=debug,livekit=info,livekit_api=info,libwebrtc=info`, which is what the tests check.
- The LiveKit SDK logs its own reconnection attempts as `ERROR` lines during an outage; they carry no content.
- Bound the journal's retention, for instance in `/etc/systemd/journald.conf.d/retention.conf`:

  ```ini
  [Journal]
  MaxRetentionSec=14day
  SystemMaxUse=500M
  ```

  then `sudo systemctl restart systemd-journald`.

## Updating

Each tag `vX.Y.Z` publishes, in its GitHub release, `livecap-x86_64-unknown-linux-gnu` and `livecap-x86_64-unknown-linux-gnu.sha256`. The update script installs one tag for one environment:

```bash
sudo install -m 0755 deploy/update.sh /usr/local/sbin/livecap-update
sudo livecap-update preprod v0.2.0
```

It downloads the binary and its checksum file, refuses the binary if the SHA-256 does not match (nothing is installed), puts it in `/opt/livecap/<environment>/releases/<tag>/`, switches the `current` symlink atomically, keeps the last three releases, and runs `systemctl restart --no-block livecap@<environment>`. The restart is a graceful stop: the running agent finishes its calls (up to two hours) before the new version starts on the same port, and meanwhile answers `503 draining` to new joins. Update `preprod` first, check `/health` reports the new version, then `production`, preferably at a quiet hour.

The checksum guards against a corrupted or truncated download; it comes from the same release as the binary, so trust rests on the GitHub repository and its release workflow.

### Rolling back

Run the script with the previous tag (`sudo livecap-update production v0.1.0`), or point `current` back at a release still on disk and restart:

```bash
sudo ln -sfn releases/v0.1.0 /opt/livecap/production/current
sudo systemctl restart --no-block livecap@production
```

## Checklist

- [ ] Two users `livecap-preprod` and `livecap-production`, two `/etc/livecap/*.env` files in mode 0600, each with its own LiveKit and Gladia keys and its own port.
- [ ] `livecap@preprod` and `livecap@production` enabled; `systemctl show -p Restart,TimeoutStopUSec livecap@production` shows `Restart=always` and a stop timeout above the drain timeout.
- [ ] Caddy serves one address per environment over HTTPS, without access logs; `/health` answers through it.
- [ ] `RUST_LOG=info` in both environment files; journal retention bounded.
- [ ] Gladia data retention: livecap deletes every session when it ends; the Gladia account settings (training on customer data) are checked separately.
