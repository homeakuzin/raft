#!/bin/bash

# Build
echo "Building showcase..."
go build -race -o showcase || { echo "Build failed!"; exit 1; }
echo "Build successful!"

# Start nodes
echo "Starting showcase dashboard..."
NODES="0:localhost:4000:localhost:4010;1:localhost:4001:localhost:4011;2:localhost:4002:localhost:4012"
./showcase $@ -nodes=$NODES
