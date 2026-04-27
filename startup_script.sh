#!/bin/bash

set -euo pipefail

PORT_DEF=${PORT:-8090}
NUM_KV=${NUM_KV:-3}
FLUSH_INTERVAL=${FLUSH_INTERVAL:-50ms}

export PEERS=kvNode1,kvNode2,kvNode3

SERVER_PIDS=()
BUILD_BIN=""

PYTHON_BIN="python3"
if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
fi

cleanup() {
    if ((${#SERVER_PIDS[@]} > 0)); then
        echo "Stopping KV-store servers..."
        kill "${SERVER_PIDS[@]}" 2>/dev/null || true
        wait "${SERVER_PIDS[@]}" 2>/dev/null || true
    fi
    if [[ -n "$BUILD_BIN" && -f "$BUILD_BIN" ]]; then
        rm -f "$BUILD_BIN"
    fi
}

trap cleanup EXIT

echo "Checking requested ports..."
for ((i=0; i<NUM_KV; i++)); do
    PORT=$((PORT_DEF + i))
    if nc -z 127.0.0.1 "$PORT" >/dev/null 2>&1; then
        echo "Port $PORT is already in use. Stop the existing process before starting the benchmark cluster."
        exit 1
    fi
done

if ! "$PYTHON_BIN" -c "import requests" >/dev/null 2>&1; then
    echo "Python package 'requests' is not available in $PYTHON_BIN."
    echo "Activate the virtual environment or install dependencies before running the benchmark."
    exit 1
fi

echo "Building KV-store server..."
BUILD_BIN=$(mktemp /tmp/kvserver.XXXXXX)
go build -o "$BUILD_BIN" .

echo "Starting Go KV-store servers..."
for ((i=0; i<NUM_KV; i++)); do
    PORT=$((PORT_DEF + i))
    NODE_NAME="kvNode$((i + 1))"
    echo "Starting $NODE_NAME on port $PORT..."

    "$BUILD_BIN" --port "$PORT" --node "$NODE_NAME" &
    PID=$!
    SERVER_PIDS+=("$PID")

    READY=0
    for _ in {1..50}; do
        if ! kill -0 "$PID" >/dev/null 2>&1; then
            echo "$NODE_NAME exited before becoming ready."
            wait "$PID"
            exit 1
        fi

        if "$PYTHON_BIN" -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:${PORT}/healthz', timeout=1).read()" >/dev/null 2>&1; then
            READY=1
            break
        fi

        sleep 0.2
    done

    if [[ "$READY" -ne 1 ]]; then
        echo "$NODE_NAME did not become ready on port $PORT."
        exit 1
    fi

    echo "$NODE_NAME ready on port $PORT."
done
echo "All servers are ready. Starting benchmark..."
"$PYTHON_BIN" benchmark.py --port "$PORT_DEF" --num-servers "$NUM_KV" --require-all-nodes
