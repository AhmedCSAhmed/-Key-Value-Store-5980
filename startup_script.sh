#!/bin/bash

PORT_DEF=${PORT:-8090}  # DEFAULT PORT
NUM_KV=${NUM_KV:-1} # Number of KV stores
echo "Starting Go KV-store servers..."

for ((i=0; i<NUM_KV; i++)); do
    PORT=$((PORT_DEF+ i))
    echo "Starting server $i on port $PORT..."

    # build server binary if not built yet
    go build -o kvserver store.go hash_ring.go

    # start server in background
    ./kvserver --port $PORT &

    SERVER_PID=$!
    echo "Waiting for server $i to start on port $PORT..."
    until nc -z localhost $PORT; do
        sleep 1
    done
    echo "Server $i ready on $PORT."
done

echo "All servers are ready. Starting benchmark..."
python3 benchmark.py --port $PORT_BASE --num-servers $NUM_KV

