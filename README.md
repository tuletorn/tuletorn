# lb — ultra-optimized Rust proxy benchmark vs. Traefik v3.7.12

Implementation of `plan.txt`: three Rust reverse-proxy data planes benchmarked
against Traefik v3.7.12 on Gateway API, standalone and on local Kubernetes.

## Workspace

| Crate | Role |
| :--- | :--- |
| `lb-core` | Routing, load balancing, filters, SIMD primitives, Gateway API reconciler |
| `lb-proxy-hyper` | Hyper 1.11 + Tokio, `SO_REUSEPORT`, jemalloc |
| `lb-proxy-pingora` | Cloudflare Pingora 0.8, jemalloc |
| `lb-proxy-monoio` | Monoio 0.2.4 thread-per-core, hybrid Tokio control plane, mimalloc |
| `lb-bench` | Harness, load generator, metrics, kind deployment, profiling, reports |

All three data planes share `lb-core`, so a measured difference reflects the
runtime and I/O model rather than a divergence in routing or filtering.

## Quick start

```bash
# Build everything
cargo build --release

# Smoke test: all three candidates, short sweep
cargo run --release --bin lb-bench -- --all --quick

# Full Scenario 1 sweep (plan §8.1)
cargo run --release --bin lb-bench -- \
    --all --scenario throughput \
    --concurrency 100,1000,5000,10000,25000 \
    --payload-sizes 1k,64k,1m \
    --http h1,h2 \
    --duration 30s --warmup 15s
```

## Verification

```bash
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all --check
RUSTFLAGS="-C target-cpu=native" cargo bench
```

## Benchmark scenarios (plan §8)

| Flag | Scenario |
| :--- | :--- |
| `--scenario throughput` | Throughput and tail latency across concurrency, payload and protocol |
| `--scenario connection-density` | C10K–C50K persistent connections, memory footprint |
| `--scenario route-churn --churn-rate 100` | Steady offered load with HTTPRoute mutations |
| `--pgo` | Standard vs. PGO build, same workload |

## Build pipelines

```bash
./scripts/build_pgo.sh lb-proxy-hyper        # three-pass PGO (plan §4.2)
./scripts/build_bolt.sh lb-proxy-hyper       # BOLT post-link, Linux only (plan §4.3)
./scripts/capture_flamegraphs.sh             # per-candidate CPU profiles (plan §5)
```

PGO needs the version-matched profiling tool: `rustup component add llvm-tools`.
A `llvm-profdata` from another LLVM will reject rustc's `.profraw` files.

Flamegraphs and BOLT both build the `profiling` profile, not `release`:
`release` sets `strip = "symbols"` and `debug = false`, which leaves every
frame unresolved and makes `llvm-bolt` refuse the binary outright. `profiling`
inherits `release` codegen and keeps the symbols.

## Kubernetes testbed (plan §5)

```bash
./scripts/setup_kind.sh                 # kind cluster + pinned Gateway API v1.2.1 CRDs
./scripts/build_and_load_images.sh      # cross-compile images, side-load into kind
kubectl apply -f k8s/traefik/           # Traefik v3.7.12 baseline
kubectl apply -f k8s/rust-proxies/      # Rust candidates
kubectl apply -f k8s/routes/benchmark-httproute.yaml

cargo run --release --bin lb-bench -- --mode k8s --all --churn-rate 50
```

Images are built inside a Linux builder container rather than by copying a host
binary in, so the flow works from macOS. Resource limits are identical for all
four candidates, and Traefik is given `GOMAXPROCS`/`GOMEMLIMIT` matching its
cgroup so the Go runtime targets its actual quota rather than the node's.

## Measurement notes

Things the harness does deliberately, because the alternative produces numbers
that look fine and mean nothing:

- **Each candidate runs as its own process.** CPU and RSS are sampled from that
  PID and its children, never from the harness. A proxy running inside the
  benchmark's own runtime would compete with the load generator for cores and
  make the memory figure a sum of three programs.
- **Equal core counts.** Every candidate defaults to the logical CPU count.
  Pingora's own `ServerConf` default is one thread, which would have it measured
  single-threaded against an eight-threaded Hyper.
- **Coordinated-omission correction.** Latency is recorded into both a raw and a
  `record_correct` histogram; the report shows both and explains which direction
  the correction moved the tail.
- **Per-worker histograms.** Recording goes through lock-free
  `hdrhistogram::sync::Recorder` handles merged at the end, so the generator does
  not bottleneck on its own mutex at high concurrency.
- **Identical proxy semantics.** All three candidates strip hop-by-hop headers,
  add `X-Forwarded-For`/`-Proto`, and apply Gateway API filters, so none of them
  is quietly doing less work per request than the Traefik baseline.

## License

MIT OR Apache-2.0.
