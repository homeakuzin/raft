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
go build -race -o $EXECUTABLE ./cmd/list || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting nodes..."
NODES="node-0:localhost:4000;node-1:localhost:4001;node-2:localhost:4002"
CLIENT_NODES="node-0:localhost:4010;node-1:localhost:4011;node-2:localhost:4012"
./$EXECUTABLE -nodes=$NODES -profileaddr=0.0.0.0:4020 -clientlisten=0.0.0.0:4010 -raftlisten=0.0.0.0:4000 -clientnodes=$CLIENT_NODES -id=node-0 -authtoken=raftlist & PIDS+=($!)
./$EXECUTABLE -nodes=$NODES -profileaddr=0.0.0.0:4021 -clientlisten=0.0.0.0:4011 -raftlisten=0.0.0.0:4001 -clientnodes=$CLIENT_NODES -id=node-1 -authtoken=raftlist & PIDS+=($!)
./$EXECUTABLE -nodes=$NODES -profileaddr=0.0.0.0:4022 -clientlisten=0.0.0.0:4012 -raftlisten=0.0.0.0:4002 -clientnodes=$CLIENT_NODES -id=node-2 -authtoken=raftlist & PIDS+=($!)

echo "Process PIDs: ${PIDS[@]}"
echo "Press Ctrl+C to stop all nodes"

# Wait for processes
wait
echo "All nodes completed."
