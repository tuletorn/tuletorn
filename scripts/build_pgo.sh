#!/usr/bin/env bash
# Three-pass Profile-Guided Optimization build (plan §4.2).
#
# The pass-2 workload is driven at the *instrumented* binary. A pipeline that
# starts the instrumented binary and then runs a benchmark which spins up its
# own in-process proxy collects an empty profile and silently produces a build
# no better than a plain release build. `lb-bench --target` is what makes the
# load land on the right process.
set -euo pipefail

CANDIDATE=${1:-lb-proxy-hyper}
PROFDATA_DIR=${PROFDATA_DIR:-target/pgo-profiles}
ROUTE_CONFIG=${ROUTE_CONFIG:-examples/pgo_routes.yaml}
TARGET_CPU=${TARGET_CPU:-native}
PROXY_PORT=${PROXY_PORT:-18090}
UPSTREAM_PORT=${UPSTREAM_PORT:-19090}
COLLECT_DURATION=${COLLECT_DURATION:-10s}

case "$CANDIDATE" in
  lb-proxy-hyper|lb-proxy-pingora|lb-proxy-monoio) ;;
  *) echo "usage: $0 <lb-proxy-hyper|lb-proxy-pingora|lb-proxy-monoio>" >&2; exit 2 ;;
esac

echo "=========================================================="
echo " PGO build: $CANDIDATE (target-cpu=$TARGET_CPU)"
echo "=========================================================="

# The llvm-profdata that ships with the rustup `llvm-tools` component is version
# matched to rustc's LLVM. A Homebrew or distro llvm-profdata is frequently a
# different major version and rejects rustc's .profraw format outright.
SYSROOT=$(rustc --print sysroot)
LLVM_PROFDATA=$(find "$SYSROOT/lib/rustlib" -name llvm-profdata -type f 2>/dev/null | head -1 || true)
if [[ -z "$LLVM_PROFDATA" ]]; then
    echo "ERROR: llvm-profdata not found in the Rust sysroot." >&2
    echo "       Install the version-matched tool:  rustup component add llvm-tools" >&2
    echo "       (A PATH llvm-profdata from another LLVM will reject rustc profiles.)" >&2
    exit 1
fi
echo "Using $LLVM_PROFDATA"

# ---------------------------------------------------------------------------
echo
echo "[Pass 1/3] Instrumented build"
rm -rf "$PROFDATA_DIR"
mkdir -p "$PROFDATA_DIR"
PROFDATA_ABS=$(cd "$PROFDATA_DIR" && pwd)

RUSTFLAGS="-C target-cpu=$TARGET_CPU -C profile-generate=$PROFDATA_ABS" \
    cargo build --release --bin "$CANDIDATE"

# Preserve the un-instrumented baseline for the Scenario 5 comparison before
# pass 3 overwrites target/release.
mkdir -p target/pgo-baseline
if [[ -x "target/release/$CANDIDATE" ]]; then
    echo "Saving the standard build for the PGO delta comparison..."
    RUSTFLAGS="-C target-cpu=$TARGET_CPU" cargo build --release --bin "$CANDIDATE" 2>/dev/null || true
    cp "target/release/$CANDIDATE" "target/pgo-baseline/$CANDIDATE"
    # Rebuild instrumented, since the line above replaced it.
    RUSTFLAGS="-C target-cpu=$TARGET_CPU -C profile-generate=$PROFDATA_ABS" \
        cargo build --release --bin "$CANDIDATE"
fi

# ---------------------------------------------------------------------------
echo
echo "[Pass 2/3] Profile collection against the instrumented binary"
cargo build --release --bin lb-mock-upstream --bin lb-bench

./target/release/lb-mock-upstream --listen "127.0.0.1:$UPSTREAM_PORT" &
MOCK_PID=$!
./target/release/"$CANDIDATE" \
    --listen "127.0.0.1:$PROXY_PORT" \
    --default-upstream "127.0.0.1:$UPSTREAM_PORT" \
    --mode standalone \
    --config "$ROUTE_CONFIG" &
PROXY_PID=$!

cleanup() {
    kill "$PROXY_PID" "$MOCK_PID" 2>/dev/null || true
    wait "$PROXY_PID" "$MOCK_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the instrumented proxy to accept before driving load at it.
for _ in $(seq 1 100); do
    if nc -z 127.0.0.1 "$PROXY_PORT" 2>/dev/null; then break; fi
    sleep 0.1
done
if ! nc -z 127.0.0.1 "$PROXY_PORT" 2>/dev/null; then
    echo "ERROR: instrumented $CANDIDATE never accepted on port $PROXY_PORT" >&2
    exit 1
fi

# --target points the generator at the instrumented process, so the profile
# reflects the real forwarding path across several payload sizes.
./target/release/lb-bench \
    --target "127.0.0.1:$PROXY_PORT" \
    --candidate "${CANDIDATE#lb-proxy-}" \
    --concurrency 100,500,1000 \
    --payload-sizes 1k,64k \
    --http h1,h2 \
    --duration "$COLLECT_DURATION" \
    --warmup 2s \
    --output-dir target/pgo-collect

cleanup
trap - EXIT

RAW_COUNT=$(find "$PROFDATA_DIR" -name '*.profraw' | wc -l | tr -d ' ')
if [[ "$RAW_COUNT" -eq 0 ]]; then
    echo "ERROR: no .profraw files were produced; the instrumented binary saw no traffic." >&2
    exit 1
fi
echo "Collected $RAW_COUNT profile files."

# ---------------------------------------------------------------------------
echo
echo "[Pass 3/3] Merging profiles and rebuilding"
"$LLVM_PROFDATA" merge -o "$PROFDATA_ABS/merged.profdata" "$PROFDATA_ABS"

RUSTFLAGS="-C target-cpu=$TARGET_CPU -C profile-use=$PROFDATA_ABS/merged.profdata" \
    cargo build --release --bin "$CANDIDATE"

echo
echo "PGO build complete:"
echo "  optimized: target/release/$CANDIDATE"
echo "  baseline:  target/pgo-baseline/$CANDIDATE"
echo
echo "Compare them with:"
echo "  ./target/release/lb-bench --pgo --candidate ${CANDIDATE#lb-proxy-}"
