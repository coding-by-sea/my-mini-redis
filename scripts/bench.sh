#!/usr/bin/env bash
# Load benchmark: my-redis against a real Redis, under maximum load.
#
#   scripts/bench.sh [OPTIONS]
#
# The benchmark starts both servers itself. Install redis-server for the comparison
# (`brew install redis`, `apt install redis-server`); without it, only my-redis
# is measured. The report is printed and written to target/bench/ (latest.md, and
# JSON for tracking over time).
#
# OPTIONS go to benches/load/ (`scripts/bench.sh --help`), for example:
#
#   scripts/bench.sh --quick                         # summary and memory only
#   scripts/bench.sh --duration 10 --clients 200
#   scripts/bench.sh --only set,get --payload 1024
set -euo pipefail
cd "$(dirname "$0")/.."

# Hundreds of connections need more file descriptors than macOS's default soft limit
# (256). Both servers are started by the benchmark, so they inherit the raised limit.
ulimit -n "$(ulimit -Hn)" 2>/dev/null || ulimit -n 10240 2>/dev/null || true

exec cargo bench --quiet --bench load -- "$@"
