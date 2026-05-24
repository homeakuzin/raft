#!/bin/bash

# Array to store process PIDs
PIDS=()

# Cleanup function
cleanup() {
    echo -e "\nInterrupt received! Stopping processes..."
    kill ${PIDS[@]} 2>/dev/null
    sleep 2
    kill -9 ${PIDS[@]} 2>/dev/null
    echo "All processes stopped."
    exit 1
}

# Set up trap
trap cleanup SIGINT SIGTERM

# Build
echo "Building..."
EXECUTABLE=list
go build -race -o $EXECUTABLE || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting nodes..."
NODES="0:localhost:4000;1:localhost:4001;2:localhost:4002"
CLIENT_NODES="0:localhost:4010;1:localhost:4011;2:localhost:4012"
./$EXECUTABLE -nodes=$NODES -clientnodes=$CLIENT_NODES -id=0 -authtoken=raftlist & PIDS+=($!)
./$EXECUTABLE -nodes=$NODES -clientnodes=$CLIENT_NODES -id=1 -authtoken=raftlist & PIDS+=($!)
./$EXECUTABLE -nodes=$NODES -clientnodes=$CLIENT_NODES -id=2 -authtoken=raftlist & PIDS+=($!)

echo "Process PIDs: ${PIDS[@]}"
echo "Press Ctrl+C to stop all nodes"

# Wait for processes
wait
echo "All nodes completed."