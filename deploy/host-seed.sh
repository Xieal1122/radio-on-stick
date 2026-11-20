#!/bin/sh
# Start a local Chromium seed for the P2P-only media mode.
# The station initiator's browser fetches /source over loopback and forwards
# audio to remote listeners; the VPS relay never receives media.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CAMPUS="$ROOT/data/campus"
PROFILE="$CAMPUS/seed-profile"
LOG="$CAMPUS/host-seed.log"
PIDFILE="$CAMPUS/host-seed.pid"
CHROME="${CHROME_BIN:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
ADDR="${RADIO_GO_ADDR:-127.0.0.1:8091}"
mkdir -p "$PROFILE"
if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "host seed already running: $(cat "$PIDFILE")"
  exit 0
fi
if [ ! -x "$CHROME" ]; then
  echo "Chromium/Chrome not found: $CHROME" >&2
  echo "Set CHROME_BIN to a Chromium executable." >&2
  exit 1
fi
nohup "$CHROME" \
  --headless=new \
  --no-sandbox \
  --disable-gpu \
  --autoplay-policy=no-user-gesture-required \
  --user-data-dir="$PROFILE" \
  --no-first-run --no-default-browser-check \
  "http://$ADDR/admin?seed=1" >>"$LOG" 2>&1 &
echo $! > "$PIDFILE"
echo "host seed started: $(cat "$PIDFILE")"
