#!/bin/bash

# Array to store process PIDs
PIDS=()

# Cleanup function
cleanup() {
    echo -e "\nInterrupt received! Stopping hyperraft processes..."
    kill ${PIDS[@]} 2>/dev/null
    sleep 2
    kill -9 ${PIDS[@]} 2>/dev/null
    echo "All hyperraft processes stopped."
    exit 1
}

# Set up trap
trap cleanup SIGINT SIGTERM

# Build
echo "Building hyperraft..."
go build -race || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting hyperraft nodes..."
NODES="0:localhost:4000;1:localhost:4001;2:localhost:4002"
./hyperraft $@ -nodes=$NODES -id=0 -metricsaddr 0.0.0.0:2112 -clientaddr 0.0.0.0:3450 & PIDS+=($!)
./hyperraft $@ -nodes=$NODES -id=1 -metricsaddr 0.0.0.0:2113 -clientaddr 0.0.0.0:3451 & PIDS+=($!)
./hyperraft $@ -nodes=$NODES -id=2 -metricsaddr 0.0.0.0:2114 -clientaddr 0.0.0.0:3452 & PIDS+=($!)

echo "Process PIDs: ${PIDS[@]}"
echo "Press Ctrl+C to stop all nodes"

# Wait for processes
wait
echo "All nodes completed."