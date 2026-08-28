#!/usr/bin/env bash
# Create the kind cluster and install the pinned Gateway API CRDs (plan §5, §7.1).
set -euo pipefail

CLUSTER_NAME=${CLUSTER_NAME:-lb-bench}
GATEWAY_API_VERSION=${GATEWAY_API_VERSION:-v1.2.1}
CRD_URL="https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"
LOCAL_CRDS="k8s/gateway-api-crds.yaml"

echo "=========================================================="
echo " kind testbed for the Gateway API benchmark"
echo "   cluster:     $CLUSTER_NAME"
echo "   node image:  kindest/node:v1.31.6"
echo "   Gateway API: $GATEWAY_API_VERSION"
echo "=========================================================="

for tool in kind kubectl docker; do
    command -v "$tool" >/dev/null 2>&1 || { echo "ERROR: $tool not found" >&2; exit 1; }
done

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    echo "Cluster '$CLUSTER_NAME' already exists."
else
    echo "Creating cluster..."
    kind create cluster --name "$CLUSTER_NAME" --config k8s/kind-cluster-config.yaml --wait 120s
fi

CONTEXT="kind-$CLUSTER_NAME"
kubectl config use-context "$CONTEXT"

# Prefer the vendored CRDs so a benchmark run is reproducible without network
# access and cannot silently pick up a different Gateway API revision.
echo
if [[ -f "$LOCAL_CRDS" ]]; then
    echo "Installing Gateway API CRDs from $LOCAL_CRDS"
    kubectl apply -f "$LOCAL_CRDS"
else
    echo "Vendored CRDs not found; fetching $GATEWAY_API_VERSION from upstream"
    curl -sSL "$CRD_URL" -o "$LOCAL_CRDS"
    kubectl apply -f "$LOCAL_CRDS"
    echo "Saved to $LOCAL_CRDS for reproducibility; commit it."
fi

echo "Waiting for the CRDs to register..."
kubectl wait --for=condition=Established --timeout=60s \
    crd/httproutes.gateway.networking.k8s.io \
    crd/gateways.gateway.networking.k8s.io \
    crd/gatewayclasses.gateway.networking.k8s.io

echo
echo "Deploying the mock upstream backends..."
kubectl apply -f k8s/mock-backend/mock-upstream-deployment.yaml
kubectl apply -f k8s/routes/churn-test-routes.yaml

echo "Waiting for backends to become ready..."
kubectl -n default rollout status deployment/mock-upstream --timeout=180s
kubectl -n default rollout status deployment/mock-upstream-b --timeout=180s

echo
echo "Testbed ready."
echo
echo "Next:"
echo "  ./scripts/build_and_load_images.sh     # build and side-load candidate images"
echo "  kubectl apply -f k8s/traefik/          # deploy the Traefik baseline"
echo "  kubectl apply -f k8s/rust-proxies/     # deploy the Rust candidates"
echo "  kubectl apply -f k8s/routes/benchmark-httproute.yaml"
kubectl get pods -A
