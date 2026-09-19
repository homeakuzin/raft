#!/usr/bin/env bash
set -euo pipefail

benchmark_id=$(date '+%Y-%m-%d_%H-%M-%S')
result_dir="./raft_benchmarks/${benchmark_id}"

# Quote arguments for the remote shell, including values containing spaces.
printf -v benchmark_command '%q ' ./raft_benchmark.sh "-benchmarkid=${benchmark_id}" "$@"
ssh raft-benchmark-host "$benchmark_command"

mkdir -p "$result_dir"
rsync -az "raft-benchmark-host:raft_benchmarks/${benchmark_id}/" "${result_dir}/"
