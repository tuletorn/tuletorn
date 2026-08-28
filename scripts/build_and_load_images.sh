#!/usr/bin/env bash
# Build every candidate container and side-load it into the kind cluster.
#
# Images are built *inside* a Linux builder container. Copying a host-built
# binary into a Debian image only works when the host is already Linux on the
# same architecture; from macOS it produces an image that cannot execute at all.
set -euo pipefail

CLUSTER_NAME=${CLUSTER_NAME:-lb-bench}
RUST_VERSION=${RUST_VERSION:-1.97.1}
PLATFORM=${PLATFORM:-}
BINARIES=${BINARIES:-"lb-proxy-hyper lb-proxy-pingora lb-proxy-monoio lb-mock-upstream"}

# kind nodes run the host's architecture, so the images must match it.
if [[ -z "$PLATFORM" ]]; then
    case "$(uname -m)" in
        arm64|aarch64) PLATFORM=linux/arm64; TARGET_CPU=${TARGET_CPU:-neoverse-n1} ;;
        x86_64|amd64)  PLATFORM=linux/amd64; TARGET_CPU=${TARGET_CPU:-x86-64-v3} ;;
        *) echo "unsupported architecture: $(uname -m)" >&2; exit 1 ;;
    esac
fi
TARGET_CPU=${TARGET_CPU:-x86-64-v3}

echo "=========================================================="
echo " Building candidate images"
echo "   platform:   $PLATFORM"
echo "   target-cpu: $TARGET_CPU"
echo "   cluster:    $CLUSTER_NAME"
echo "=========================================================="

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running." >&2
    exit 1
fi

for binary in $BINARIES; do
    echo
    echo "--- $binary ---"
    docker build \
        --platform "$PLATFORM" \
        --build-arg "BINARY=$binary" \
        --build-arg "RUST_VERSION=$RUST_VERSION" \
        --build-arg "TARGET_CPU=$TARGET_CPU" \
        -f docker/Dockerfile \
        -t "$binary:latest" \
        .
done

# Pull the pinned Traefik image so the benchmark never races a registry fetch
# during a measurement window.
echo
echo "--- traefik:v3.7.12 ---"
docker pull --platform "$PLATFORM" traefik:v3.7.12

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    echo
    echo "Cluster '$CLUSTER_NAME' does not exist; run ./scripts/setup_kind.sh first."
    exit 1
fi

echo
echo "Loading images into kind cluster '$CLUSTER_NAME'..."
for binary in $BINARIES; do
    kind load docker-image "$binary:latest" --name "$CLUSTER_NAME"
done
kind load docker-image traefik:v3.7.12 --name "$CLUSTER_NAME"

echo
echo "All images loaded. Deploy with:"
echo "  kubectl apply -f k8s/rust-proxies/"
