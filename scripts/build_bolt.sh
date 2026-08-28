#!/usr/bin/env bash
# BOLT post-link optimization (plan §4.3). Linux only.
#
# Two prerequisites that the release profile in plan §2 actively breaks:
#   * `strip = "symbols"` removes the symbol table BOLT needs.
#   * BOLT needs relocations, which require linking with --emit-relocs.
# Both are handled by building the `profiling` profile with an extra link flag.
set -euo pipefail

CANDIDATE=${1:-lb-proxy-hyper}
PROXY_PORT=${PROXY_PORT:-18090}
UPSTREAM_PORT=${UPSTREAM_PORT:-19090}
RECORD_SECONDS=${RECORD_SECONDS:-30}
TARGET_CPU=${TARGET_CPU:-native}

if [[ "$(uname -s)" != "Linux" ]]; then
    echo "BOLT requires Linux perf branch sampling; skipping on $(uname -s)."
    echo "(plan §4.3 notes this is auto-skipped on macOS.)"
    exit 0
fi

for tool in llvm-bolt perf2bolt perf; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: $tool not found. Install LLVM 17+ with BOLT and linux-perf." >&2
        exit 1
    fi
done

echo "=========================================================="
echo " BOLT post-link optimization: $CANDIDATE"
echo "=========================================================="

# `profiling` inherits release codegen but keeps symbols; --emit-relocs makes
# the binary rewritable by BOLT.
echo "[1/4] Building with symbols and relocations"
RUSTFLAGS="-C target-cpu=$TARGET_CPU -C link-arg=-Wl,--emit-relocs" \
    cargo build --profile profiling --bin "$CANDIDATE"
cargo build --profile profiling --bin lb-mock-upstream --bin lb-bench

BINARY="target/profiling/$CANDIDATE"
if ! nm "$BINARY" >/dev/null 2>&1; then
    echo "ERROR: $BINARY has no symbol table; BOLT cannot rewrite it." >&2
    exit 1
fi

echo "[2/4] Recording a representative profile with perf"
./target/profiling/lb-mock-upstream --listen "127.0.0.1:$UPSTREAM_PORT" &
MOCK_PID=$!
"$BINARY" \
    --listen "127.0.0.1:$PROXY_PORT" \
    --default-upstream "127.0.0.1:$UPSTREAM_PORT" \
    --mode standalone \
    --config examples/pgo_routes.yaml &
PROXY_PID=$!
trap 'kill $PROXY_PID $MOCK_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
    if nc -z 127.0.0.1 "$PROXY_PORT" 2>/dev/null; then break; fi
    sleep 0.1
done

# `-j any,u` collects last-branch records, which is what perf2bolt needs to
# reconstruct the control-flow profile. Plain cycle sampling is not enough.
perf record -e cycles:u -j any,u -o perf.data -p "$PROXY_PID" -- \
    ./target/profiling/lb-bench \
        --target "127.0.0.1:$PROXY_PORT" \
        --concurrency 1000 \
        --payload-sizes 1k \
        --duration "${RECORD_SECONDS}s" \
        --warmup 5s \
        --output-dir target/bolt-collect

kill "$PROXY_PID" "$MOCK_PID" 2>/dev/null || true
trap - EXIT

echo "[3/4] Converting the perf profile"
perf2bolt -p perf.data -o perf.fdata "$BINARY"

echo "[4/4] Rewriting the binary"
llvm-bolt "$BINARY" -o "$BINARY.bolt" \
    -data=perf.fdata \
    -reorder-blocks=ext-tsp \
    -reorder-functions=hfsort \
    -split-functions \
    -split-all-cold \
    -split-eh \
    -dyno-stats

echo
echo "BOLT build complete: $BINARY.bolt"
echo "Benchmark it with:"
echo "  ./target/profiling/lb-bench --target 127.0.0.1:$PROXY_PORT"
