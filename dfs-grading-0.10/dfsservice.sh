#!/bin/bash
# DFS Service start/stop (Linux/Mac)
# Usage:
#   ./dfsservice.sh start <dfsPort> <extent_host:port> <lock_host:port>
#   ./dfsservice.sh stop  <host:port>

set -euo pipefail

ACTION="${1:-}"
ARG1="${2:-}"   # port or host:port
ARG2="${3:-}"   # extent addr:port (start only)
ARG3="${4:-}"   # lock addr:port (start only)

MAIN_CLASS="sk.tuke.dfs.dfs.DfsServer"

# Locate repo root (pom.xml)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
if [[ ! -f "$PROJECT_DIR/pom.xml" ]]; then
  for up in .. ../.. ../../..; do
    if [[ -f "$SCRIPT_DIR/$up/pom.xml" ]]; then PROJECT_DIR="$(cd "$SCRIPT_DIR/$up" && pwd)"; break; fi
  done
fi
if [[ ! -f "$PROJECT_DIR/pom.xml" ]]; then
  echo "pom.xml not found. Run from repo or place script inside the repo tree." >&2
  exit 1
fi

PID_FILE="$PROJECT_DIR/dfsservice.pid"
LOG_DIR="$PROJECT_DIR/logs"
LOG_OUT="$LOG_DIR/dfsservice.log"
LOG_ERR="$LOG_DIR/dfsservice.err"
mkdir -p "$LOG_DIR"

port_listening() {
  local p="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1
  else
    netstat -an 2>/dev/null | grep -qE "[:.]$p[[:space:]].*LISTEN"
  fi
}

case "$ACTION" in
  start)
    DFS_PORT="$ARG1"
    EXTENT_ADDR="$ARG2"
    LOCK_ADDR="$ARG3"

    if [[ -z "${DFS_PORT}" || -z "${EXTENT_ADDR}" || -z "${LOCK_ADDR}" ]]; then
      echo "Usage: $0 start <dfsPort> <extent_host:port> <lock_host:port>"
      exit 1
    fi
    if ! [[ "$DFS_PORT" =~ ^[0-9]+$ ]] || (( DFS_PORT < 1 || DFS_PORT > 65535 )); then
      echo "Invalid dfsPort: $DFS_PORT" >&2
      exit 2
    fi

    echo "Starting DFS on $DFS_PORT (extent=$EXTENT_ADDR, lock=$LOCK_ADDR)…"
    (
      cd "$PROJECT_DIR"
      mvn -q -DskipTests compile
      nohup mvn -q -DskipTests exec:java \
        -Dexec.mainClass="$MAIN_CLASS" \
        -Dexec.classpathScope=runtime \
        -Dexec.args="$DFS_PORT $EXTENT_ADDR $LOCK_ADDR" \
        >>"$LOG_OUT" 2>>"$LOG_ERR" &
      echo $! > "$PID_FILE"
    )

    # Wait up to ~5s for port to listen
    for _ in {1..50}; do
      sleep 0.1
      if port_listening "$DFS_PORT"; then
        echo "DFS started. PID $(cat "$PID_FILE"). Logs: $LOG_OUT"
        exit 0
      fi
    done

    echo "DFS failed to start on $DFS_PORT. Check logs: $LOG_OUT $LOG_ERR" >&2
    exit 3
    ;;

  stop)
    ADDRESS="$ARG1"
    if [[ -z "$ADDRESS" ]]; then
      echo "Usage: $0 stop <host:port>"
      exit 1
    fi
    PORT="$(echo "$ADDRESS" | awk -F: '{print $2}')"

    if [[ -f "$PID_FILE" ]]; then
      PID="$(cat "$PID_FILE")"
      kill "$PID" 2>/dev/null || true
      rm -f "$PID_FILE"
      echo "Stopped DFS PID $PID"
      exit 0
    fi

    # Fallback: kill by port
    if [[ -n "$PORT" ]]; then
      if command -v lsof >/dev/null 2>&1; then
        PID="$(lsof -ti tcp:$PORT || true)"
      else
        PID="$(ps aux | grep "$MAIN_CLASS" | grep -v grep | awk '{print $2}' | head -n1)"
      fi
      if [[ -n "$PID" ]]; then
        kill "$PID" 2>/dev/null || true
        echo "Stopped DFS PID $PID on port $PORT"
      else
        echo "No process found on port $PORT"
      fi
      exit 0
    fi
    ;;

  *)
    echo "Usage:"
    echo "  $0 start <dfsPort> <extent_host:port> <lock_host:port>"
    echo "  $0 stop <host:port>"
    exit 1
    ;;
esac
