#!/usr/bin/env bash
# Compare my-mini-redis builds with the load benchmark, resumably.
#
#   scripts/compare.sh [--reps N] [--perf] <results dir> <cases file> <name>=<server binary>...
#
# Each line of the cases file is `<case name> <benchmark arguments>`; `#` starts a
# comment. For every case and repetition, every build runs in turn, so drift during
# the session affects them alike. A build is any my-mini-redis server binary, for
# example one built from another commit:
#
#   git worktree add ../base main && cargo build --release --manifest-path ../base/Cargo.toml
#   scripts/compare.sh results cases.txt base=../base/target/release/my-mini-redis-server \
#       new=target/release/my-mini-redis-server
#   scripts/summarize.py results
#
# Every run keeps its raw output in <results dir>/<case>/<name>/rep<N>/: the benchmark's
# JSON and Markdown reports, its log, and meta.json (arguments, binary hash, commit). The
# benchmark starts the binary fresh for each run and measures its CPU and memory.
# Nothing is parsed here; scripts/summarize.py does that, and can be fixed and re-run on
# the same results.
#
# Resumable: a run is complete once its `done` marker exists, and complete runs are
# skipped, so running the same command again after an interruption, a failure or with
# a higher --reps continues where it stopped.
#
# --perf (Linux): also counts the server's syscalls with `perf stat`, over 5 s starting
# 3 s into the run, so give cases at least --duration 8 (with the default 1 s warmup).
set -uo pipefail
cd "$(dirname "$0")/.."

reps=2
perf=false
while [ $# -gt 0 ]; do
  case $1 in
    --reps) reps=$2; shift 2 ;;
    --perf) perf=true; shift ;;
    -h|--help) sed -n '2,/^set /p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
    *) break ;;
  esac
done
if [ $# -lt 3 ]; then
  echo "usage: $0 [--reps N] [--perf] <results dir> <cases file> <name>=<server binary>..." >&2
  exit 2
fi
root=$1 cases=$2; shift 2
builds=("$@")
for build in "${builds[@]}"; do
  binary=${build#*=}
  if [ "$build" = "$binary" ] || [ ! -x "$binary" ]; then
    echo "not a <name>=<executable server binary>: $build" >&2
    exit 2
  fi
done
if $perf && ! command -v perf >/dev/null; then
  echo "--perf needs Linux perf" >&2
  exit 2
fi

# Hundreds of connections need more file descriptors than macOS's default limit.
ulimit -n "$(ulimit -Hn)" 2>/dev/null || ulimit -n 10240 2>/dev/null || true
cargo bench --quiet --bench load --no-run || exit 1

sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
json_string() { printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"; }

# measure <case> <name> <binary> <rep> <benchmark arguments...>
measure() {
  local case=$1 name=$2 binary=$3 rep=$4; shift 4
  local dir=$root/$case/$name/rep$rep
  if [ -e "$dir/done" ]; then
    echo "skip   $case/$name/rep$rep"
    return 0
  fi
  rm -rf "$dir"
  mkdir -p "$dir"
  {
    printf '{\n  "case": %s,\n  "build": %s,\n  "rep": %s,\n' "$(json_string "$case")" "$(json_string "$name")" "$rep"
    printf '  "binary": %s,\n  "binary_sha256": "%s",\n' "$(json_string "$binary")" "$(sha256 "$binary")"
    printf '  "commit": %s,\n' "$(json_string "$(git describe --always --dirty 2>/dev/null || echo unknown)")"
    printf '  "arguments": %s,\n  "started_at": "%s"\n}\n' "$(json_string "$*")" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } > "$dir/meta.json"

  cargo bench --quiet --bench load -- --quick --no-redis --no-memory --server "$binary" \
    --report-dir "$dir/bench" "$@" > "$dir/bench.log" 2>&1 &
  local bench=$!
  if $perf; then
    # The benchmark starts the server; find it among the benchmark's descendants.
    sleep 3
    local server
    server=$(pgrep -n -f "^$(realpath "$binary")$|^$binary$") || true
    if [ -n "$server" ]; then
      perf stat -x, -p "$server" -o "$dir/perf.csv" -e task-clock,context-switches,syscalls:sys_enter_sendto,syscalls:sys_enter_recvfrom,syscalls:sys_enter_futex,syscalls:sys_enter_epoll_pwait -- sleep 5 2>/dev/null || true
    fi
  fi
  if ! wait $bench || [ ! -s "$dir/bench/latest.json" ]; then
    echo "FAILED $case/$name/rep$rep: see $dir/bench.log"
    return 1
  fi
  touch "$dir/done"
  echo "done   $case/$name/rep$rep"
}

failed=0
while read -r case args; do
  case $case in ''|'#'*) continue ;; esac
  for rep in $(seq 1 "$reps"); do
    for build in "${builds[@]}"; do
      # shellcheck disable=SC2086 # the case's arguments are meant to split
      measure "$case" "${build%%=*}" "${build#*=}" "$rep" $args || failed=$((failed + 1))
    done
  done
done < "$cases"
echo "finished: $failed failed run(s); re-run the same command to retry them"
[ $failed -eq 0 ]
