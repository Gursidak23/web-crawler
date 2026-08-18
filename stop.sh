#!/usr/bin/env bash
set -euo pipefail

# stop.sh - stop the local crawler server
# Usage: ./stop.sh [port]
# Default port: 8020

PORT=${1:-8010}

echo "Stopping processes listening on :$PORT..."

# Find PIDs listening on the TCP port (compatible with macOS/linux)
PIDS=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null || true)

if [ -z "$PIDS" ]; then
  echo "No process listening on :$PORT"
  exit 0
fi

echo "Found PIDs: $PIDS"

# Try graceful termination first
for pid in $PIDS; do
  echo "Sending SIGTERM to $pid"
  kill "$pid" 2>/dev/null || true
done

# Wait up to 10 seconds for processes to exit
for i in $(seq 1 10); do
  sleep 1
  REMAIN=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null || true)
  if [ -z "$REMAIN" ]; then
    echo "Processes stopped."
    exit 0
  fi
  echo "Still running: $REMAIN"
done

echo "Graceful stop failed; forcing kill..."
for pid in $PIDS; do
  echo "Killing $pid"
  kill -9 "$pid" 2>/dev/null || true
done

sleep 1
REMAIN=$(lsof -nP -iTCP:"$PORT" -sTCP:LISTEN -t 2>/dev/null || true)
if [ -z "$REMAIN" ]; then
  echo "Processes killed."
  exit 0
else
  echo "Failed to stop processes on :$PORT. Remaining: $REMAIN" >&2
  exit 1
fi
