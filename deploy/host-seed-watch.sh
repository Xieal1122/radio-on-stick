#!/bin/sh
# Keep the local P2P seed alive. This does not carry media through the VPS.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/data/campus/host-seed-watch.pid"
SELF="$$"
if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "host seed watchdog already running: $(cat "$PIDFILE")"
  exit 0
fi
echo "$SELF" > "$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT INT TERM
while :; do
  if [ -f "$ROOT/data/campus/host-seed.pid" ] && kill -0 "$(cat "$ROOT/data/campus/host-seed.pid")" 2>/dev/null; then
    :
  else
    bash "$ROOT/deploy/host-seed.sh" || true
  fi
  sleep 10
done
