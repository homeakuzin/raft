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
go build || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting hyperraft nodes..."
./hyperraft 0 & PIDS+=($!)
./hyperraft 1 & PIDS+=($!)
./hyperraft 2 & PIDS+=($!)

echo "Process PIDs: ${PIDS[@]}"
echo "Press Ctrl+C to stop all nodes"

# Wait for processes
wait
echo "All nodes completed."