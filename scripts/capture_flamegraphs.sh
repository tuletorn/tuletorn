#!/usr/bin/env bash
# Automated flamegraph capture for every candidate (plan §5).
#
# Builds the `profiling` profile rather than `release`: the release profile sets
# `strip = "symbols"` and `debug = false`, which turns every flamegraph frame
# into an unresolved hexadecimal address.
set -euo pipefail

CONCURRENCY=${CONCURRENCY:-1000}
DURATION=${DURATION:-30}
OUTPUT_DIR=${OUTPUT_DIR:-results/flamegraphs}
UPSTREAM_PORT=${UPSTREAM_PORT:-19090}
TRAEFIK_PPROF=${TRAEFIK_PPROF:-http://127.0.0.1:8009}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --concurrency) CONCURRENCY=$2; shift 2 ;;
        --duration)    DURATION=$2; shift 2 ;;
        --output-dir)  OUTPUT_DIR=$2; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

if ! command -v flamegraph >/dev/null 2>&1; then
    echo "ERROR: cargo-flamegraph not installed. Run: cargo install flamegraph" >&2
    exit 1
fi

echo "=========================================================="
echo " Flamegraph capture (c=$CONCURRENCY, ${DURATION}s)"
echo "=========================================================="

echo "Building the profiling profile (symbols retained)..."
cargo build --profile profiling \
    --bin lb-proxy-hyper --bin lb-proxy-pingora --bin lb-proxy-monoio \
    --bin lb-mock-upstream --bin lb-bench

./target/profiling/lb-mock-upstream --listen "127.0.0.1:$UPSTREAM_PORT" &
MOCK_PID=$!
trap 'kill $MOCK_PID 2>/dev/null || true' EXIT

capture_rust() {
    local name=$1 port=$2
    echo
    echo "--- $name ---"
    ./target/profiling/"$name" \
        --listen "127.0.0.1:$port" \
        --default-upstream "127.0.0.1:$UPSTREAM_PORT" \
        --mode standalone &
    local pid=$!

    for _ in $(seq 1 100); do
        if nc -z 127.0.0.1 "$port" 2>/dev/null; then break; fi
        sleep 0.1
    done

    # Drive load in the background; sample the proxy while it is under it.
    ./target/profiling/lb-bench \
        --target "127.0.0.1:$port" \
        --concurrency "$CONCURRENCY" \
        --payload-sizes 1k \
        --duration "$((DURATION + 10))s" \
        --warmup 5s \
        --output-dir target/flamegraph-load >/dev/null 2>&1 &
    local load_pid=$!
    sleep 5

    # 997 Hz: prime, so sampling cannot alias with a periodic workload.
    flamegraph --pid "$pid" --freq 997 \
        --output "$OUTPUT_DIR/$name-c$CONCURRENCY.svg" \
        -- sleep "$DURATION" || echo "WARNING: capture failed for $name"

    kill "$load_pid" "$pid" 2>/dev/null || true
    wait "$load_pid" "$pid" 2>/dev/null || true
    echo "Wrote $OUTPUT_DIR/$name-c$CONCURRENCY.svg"
}

capture_rust lb-proxy-hyper   18080
capture_rust lb-proxy-pingora 18081
capture_rust lb-proxy-monoio  18082

# Traefik is Go, so it is profiled through its own pprof endpoint rather than
# with perf/dtrace.
if command -v go >/dev/null 2>&1 && curl -sf "$TRAEFIK_PPROF/ping" >/dev/null 2>&1; then
    echo
    echo "--- traefik (go pprof) ---"
    go tool pprof -svg \
        -output "$OUTPUT_DIR/traefik-v3.7.12-c$CONCURRENCY.svg" \
        "$TRAEFIK_PPROF/debug/pprof/profile?seconds=$DURATION" \
        && echo "Wrote $OUTPUT_DIR/traefik-v3.7.12-c$CONCURRENCY.svg"
else
    echo
    echo "Skipping Traefik pprof: needs the Go toolchain and Traefik reachable at $TRAEFIK_PPROF"
fi

echo
echo "Flamegraphs in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"
