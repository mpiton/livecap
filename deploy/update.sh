#!/usr/bin/env bash
# Installs a tagged livecap release for one environment and restarts it gracefully.
#
#   sudo ./update.sh <environment> <tag>      e.g. sudo ./update.sh preprod v0.2.0
#
# Downloads livecap-x86_64-unknown-linux-gnu and its .sha256 from the GitHub release, refuses
# the binary unless the checksum matches, installs it under
# /opt/livecap/<environment>/releases/<tag>/, points /opt/livecap/<environment>/current at it
# and restarts livecap@<environment>. The restart is graceful: the running agent stops taking
# rooms and lets its calls end (up to LIVECAP_DRAIN_TIMEOUT_SECS) before the new one starts.
#
# For tests only: LIVECAP_RELEASES_URL replaces https://github.com/<repo>/releases/download,
# LIVECAP_PREFIX replaces /opt/livecap, SYSTEMCTL replaces systemctl.
set -euo pipefail

usage() {
  echo "usage: $0 <environment> <tag>   (e.g. $0 preprod v0.2.0)" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
environment=$1
tag=$2
[[ $environment =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "invalid environment: $environment" >&2; exit 2; }
[[ $tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] || { echo "invalid tag: $tag" >&2; exit 2; }

repo=${LIVECAP_REPO:-mpiton/livecap}
releases_url=${LIVECAP_RELEASES_URL:-https://github.com/$repo/releases/download}
prefix=${LIVECAP_PREFIX:-/opt/livecap}
systemctl=${SYSTEMCTL:-systemctl}
asset=livecap-x86_64-unknown-linux-gnu
unit=livecap@$environment.service
base=$prefix/$environment
# Releases kept on disk, the new one included, for a quick rollback.
keep=3

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

echo "downloading $tag"
curl --fail --silent --show-error --location --retry 3 \
  --output "$work/$asset" "$releases_url/$tag/$asset"
curl --fail --silent --show-error --location --retry 3 \
  --output "$work/$asset.sha256" "$releases_url/$tag/$asset.sha256"

# The checksum file names the asset: `<sha256>  livecap-x86_64-unknown-linux-gnu`.
if ! (cd "$work" && sha256sum --check --strict --status "$asset.sha256"); then
  echo "checksum mismatch for $asset $tag: nothing installed" >&2
  exit 1
fi
echo "checksum verified"

install -d -m 0755 "$base/releases/$tag"
install -m 0755 "$work/$asset" "$base/releases/$tag/livecap"
# Atomic switch: a new symlink renamed over the old one.
ln -sfn "releases/$tag" "$base/current.new"
mv -T "$base/current.new" "$base/current"
echo "installed $base/releases/$tag/livecap"

# Old releases: the running process keeps its open binary even if its directory goes.
(cd "$base/releases" && ls -1t | tail -n +$((keep + 1)) | while read -r old; do
  [[ $old == "$tag" ]] || rm -rf -- "$old"
done)

# --no-block: the stop drains calls in progress, which can take up to two hours.
"$systemctl" restart --no-block "$unit"
echo "restart of $unit requested: the running agent drains its calls, then $tag starts."
echo "follow it with: journalctl -u $unit -f"
