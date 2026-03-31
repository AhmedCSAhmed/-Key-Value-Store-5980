#!/bin/bash

PORT_DEF=${PORT:-8090}  # DEFAULT PORT
NUM_KV=${NUM_KV:-3}     # Number of KV stores
echo "Starting Go KV-store servers..."

# Build once before the loop
go build -o kvserver store.go hash_ring.go

for ((i=0; i<NUM_KV; i++)); do
    PORT=$((PORT_DEF + i))
    NODE_NAME="kvNode$((i + 1))"
    echo "Starting server $i ($NODE_NAME) on port $PORT..."

    # start server in background
    ./kvserver --port $PORT --node $NODE_NAME &

    echo "Waiting for server $i to start on port $PORT..."
    until nc -z localhost $PORT; do
        sleep 1
    done
    echo "Server $i ready on $PORT."
done

echo "All servers are ready. Starting benchmark..."
python3 benchmark.py --port $PORT_DEF --num-servers $NUM_KV

