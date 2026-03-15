#!/bin/bash

echo "Starting Go KV-store server..."

go run store.go &

SERVER_PID=$!

echo "Waiting for server to start..."

until nc -z localhost 8090; do
   sleep 2
done

echo "Server Ready, Starting benchmark..."

python3 benchmark.py
