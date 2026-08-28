# syntax=docker/dockerfile:1
#
# Multi-stage build for every lb binary.
#
# The build stage cross-compiles inside Linux rather than copying a host binary
# in. A macOS `cargo build --release` produces a Mach-O arm64 executable, which
# cannot run in a Linux container at all — that was the reason the previous
# image-build script could never have worked from a Mac.
#
# `target-cpu` is a portable-but-modern baseline, not `native`: a kind node may
# be scheduled on a different microarchitecture than the machine that built it,
# and an illegal-instruction crash inside the cluster is very hard to diagnose.

ARG RUST_VERSION=1.97.1
FROM rust:${RUST_VERSION}-slim-bookworm AS builder

ARG BINARY=lb-proxy-hyper
ARG TARGET_CPU=x86-64-v3

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev cmake make g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first so dependency compilation is cached independently of
# source changes.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/core/Cargo.toml crates/core/
COPY crates/proxy-hyper/Cargo.toml crates/proxy-hyper/
COPY crates/proxy-pingora/Cargo.toml crates/proxy-pingora/
COPY crates/proxy-monoio/Cargo.toml crates/proxy-monoio/
COPY crates/bench/Cargo.toml crates/bench/

# Stub sources so `cargo build` resolves and compiles the dependency graph.
RUN set -eux; \
    for c in core proxy-hyper proxy-pingora proxy-monoio bench; do \
        mkdir -p "crates/$c/src"; \
        echo "" > "crates/$c/src/lib.rs"; \
    done; \
    for c in proxy-hyper proxy-pingora proxy-monoio bench; do \
        echo "fn main() {}" > "crates/$c/src/main.rs"; \
    done; \
    mkdir -p crates/bench/src/bin; \
    echo "fn main() {}" > crates/bench/src/bin/lb-mock-upstream.rs; \
    mkdir -p crates/core/benches; \
    for b in routing_bench route_swap_bench simd_parsing_bench; do \
        echo "fn main() {}" > "crates/core/benches/$b.rs"; \
    done

ENV RUSTFLAGS="-C target-cpu=${TARGET_CPU} -C llvm-args=-inline-threshold=600"
RUN cargo build --release --workspace 2>/dev/null || true

# Now the real sources. Touching them invalidates only the workspace crates.
COPY crates/ crates/
RUN find crates -name '*.rs' -newermt '1 second ago' -exec touch {} +

RUN cargo build --release --bin "${BINARY}"
RUN cp "target/release/${BINARY}" /usr/local/bin/lb-binary

# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 65532 --no-create-home lb

COPY --from=builder /usr/local/bin/lb-binary /usr/local/bin/lb-binary

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/lb-binary"]
