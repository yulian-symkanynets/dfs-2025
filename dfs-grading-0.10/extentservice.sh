#!/bin/bash
# Simple start/stop for ExtentService (no JAR needed)
# Usage:
#   ./extentservice.sh start 1002 /path/to/extent-root
#   ./extentservice.sh stop 127.0.0.1:1002

set -euo pipefail

ACTION="${1:-}"
ARG1="${2:-}"   # port or host:port
ARG2="${3:-}"   # extent root (for start)

MAIN_CLASS="sk.tuke.dfs.extent.ExtentServer"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
if [[ ! -f "$PROJECT_DIR/pom.xml" ]]; then
  for up in .. ../.. ../../..; do
    if [[ -f "$SCRIPT_DIR/$up/pom.xml" ]]; then PROJECT_DIR="$(cd "$SCRIPT_DIR/$up" && pwd)"; break; fi
  done
fi
if [[ ! -f "$PROJECT_DIR/pom.xml" ]]; then
  echo "pom.xml not found. Run this from the repo or place it in the repo tree." >&2
  exit 1
fi

PID_FILE="$PROJECT_DIR/extentservice.pid"
LOG_DIR="$PROJECT_DIR/logs"
LOG_OUT="$LOG_DIR/extentservice.log"
mkdir -p "$LOG_DIR"

port_in_use() {
  local p="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"$p" >/dev/null 2>&1
  else
    netstat -an 2>/dev/null | grep -qE "[:.]$p[[:space:]].*LISTEN"
  fi
}

case "$ACTION" in
  start)
    PORT="$ARG1"
    EXTENT_ROOT="$ARG2"

    if [[ -z "${PORT}" || -z "${EXTENT_ROOT}" ]]; then
      echo "Usage: $0 start <port> <extent-root-path>"
      exit 1
    fi
    if ! [[ "$PORT" =~ ^[0-9]+$ ]] || (( PORT < 1 || PORT > 65535 )); then
      echo "Invalid port: $PORT" >&2
      exit 2
    fi
    if port_in_use "$PORT"; then
      echo "Port $PORT is already in use." >&2
      exit 3
    fi

    # Ensure extent root exists
    mkdir -p "$EXTENT_ROOT"

    echo "Starting ExtentService on port $PORT with root '$EXTENT_ROOT' (project: $PROJECT_DIR)…"

    (
      cd "$PROJECT_DIR"
      # Build classes first (faster subsequent runs), then launch
      mvn -q -DskipTests compile

      # Pass BOTH args: <port> <extentRoot>
      nohup mvn -q -DskipTests exec:java \
        -Dexec.mainClass="$MAIN_CLASS" \
        -Dexec.classpathScope=runtime \
        -Dexec.args="$PORT $EXTENT_ROOT" \
        >> "$LOG_OUT" 2>&1 &

      echo $! > "$PID_FILE"
    )

    # Tiny wait & sanity check
    sleep 0.4
    if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "Failed to start. See logs: $LOG_OUT" >&2
      exit 4
    fi
    echo "Started. PID $(cat "$PID_FILE"). Logs: $LOG_OUT"
    ;;

  stop)
    ADDRESS="$ARG1"
    if [[ -z "$ADDRESS" ]]; then
      echo "Usage: $0 stop <host:port>"
      exit 1
    fi

    PORT="$(echo "$ADDRESS" | awk -F: '{print $2}')"
    echo "Stopping ExtentService (address: $ADDRESS)…"

    if [[ -f "$PID_FILE" ]]; then
      PID="$(cat "$PID_FILE")"
      kill "$PID" 2>/dev/null || true
      rm -f "$PID_FILE"
      echo "Stopped PID $PID"
      exit 0
    fi

    if [[ -n "$PORT" ]]; then
      if command -v lsof >/dev/null 2>&1; then
        PID="$(lsof -ti tcp:$PORT || true)"
      else
        PID="$(ps aux | grep "$MAIN_CLASS" | grep -v grep | awk '{print $2}' | head -n1)"
      fi
      if [[ -n "$PID" ]]; then
        kill "$PID" 2>/dev/null || true
        echo "Stopped PID $PID on port $PORT"
      else
        echo "No process found on port $PORT"
      fi
      exit 0
    fi

    echo "Usage: $0 stop <host:port>"
    exit 1
    ;;

  *)
    echo "Usage:"
    echo "  $0 start <port> <extent-root-path>"
    echo "  $0 stop <host:port>"
    exit 1
    ;;
esac
