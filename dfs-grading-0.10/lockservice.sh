#!/bin/bash
# Simple start/stop for LockService (no JAR needed)
# Usage:
#   ./lockservice.sh start 1001
#   ./lockservice.sh stop 127.0.0.1:1001

set -e

ACTION="$1"
ARG="$2"

# Adjust if your main class differs:
MAIN_CLASS="sk.tuke.dfs.lock.LockServer"

# Resolve project root (where pom.xml is). Assumes this script is at repo root,
# but also works if placed in subfolder (climbs up a few levels).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
if [ ! -f "$PROJECT_DIR/pom.xml" ]; then
  for up in .. ../.. ../../..; do
    if [ -f "$SCRIPT_DIR/$up/pom.xml" ]; then PROJECT_DIR="$(cd "$SCRIPT_DIR/$up" && pwd)"; break; fi
  done
fi
if [ ! -f "$PROJECT_DIR/pom.xml" ]; then
  echo "pom.xml not found. Run this script from the repo or move it to the repo root." >&2
  exit 1
fi

PID_FILE="$PROJECT_DIR/lockservice.pid"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

case "$ACTION" in
  start)
    PORT="$ARG"
    if [ -z "$PORT" ]; then echo "Usage: $0 start <port>"; exit 1; fi

    echo "Starting LockService on port $PORT (project: $PROJECT_DIR)…"
    # Run in background via Maven exec, log to file, remember PID
    (
      cd "$PROJECT_DIR"
      nohup mvn -q -DskipTests compile exec:java \
        -Dexec.mainClass="$MAIN_CLASS" \
        -Dexec.args="$PORT" \
        > "$LOG_DIR/lockservice.log" 2>&1 &
      echo $! > "$PID_FILE"
    )
    sleep 0.3
    echo "Started. PID $(cat "$PID_FILE"). Logs: $LOG_DIR/lockservice.log"
    ;;

  stop)
    ADDRESS="$ARG"
    PORT="$(echo "$ADDRESS" | awk -F: '{print $2}')"
    echo "Stopping LockService (address: $ADDRESS)…"

    if [ -f "$PID_FILE" ]; then
      PID="$(cat "$PID_FILE")"
      kill "$PID" 2>/dev/null || true
      rm -f "$PID_FILE"
      echo "Stopped PID $PID"
    elif [ -n "$PORT" ]; then
      # Fallback: kill by listening port
      PID="$(lsof -ti tcp:$PORT || true)"
      if [ -n "$PID" ]; then
        kill "$PID" 2>/dev/null || true
        echo "Stopped PID $PID on port $PORT"
      else
        echo "No process found on port $PORT"
      fi
    else
      echo "Usage: $0 stop <host:port>"
      exit 1
    fi
    ;;

  *)
    echo "Usage:"
    echo "  $0 start <port>"
    echo "  $0 stop <host:port>"
    exit 1
    ;;
esac
