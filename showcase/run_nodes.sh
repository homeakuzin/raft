#!/bin/bash

# Array to store process PIDs
PIDS=()

# Cleanup function
cleanup() {
    echo -e "\nInterrupt received! Stopping showcase processes..."
    kill ${PIDS[@]} 2>/dev/null
    sleep 2
    kill -9 ${PIDS[@]} 2>/dev/null
    echo "All showcase processes stopped."
    exit 1
}

# Set up trap
trap cleanup SIGINT SIGTERM

# Build
echo "Building showcase..."
go build -race -o showcase || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting showcase nodes..."
NODES="0:localhost:4000:localhost:4010;1:localhost:4001:localhost:4011;2:localhost:4002:localhost:4012"
./showcase $@ -nodes=$NODES -id=0 & PIDS+=($!)
./showcase $@ -nodes=$NODES -id=1 & PIDS+=($!)
./showcase $@ -nodes=$NODES -id=2 & PIDS+=($!)

echo "Process PIDs: ${PIDS[@]}"
echo "Press Ctrl+C to stop all nodes"

# Wait for processes
wait
echo "All nodes completed."