#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

kind_bin="${KIND:-kind}"
kubectl_bin="${KUBECTL:-kubectl}"
container_tool="${CONTAINER_TOOL:-docker}"
cluster="${KIND_CLUSTER:-churnless}"
image="${KIND_IMAGE:-example.com/churnless:v0.0.1}"
timeout="${KIND_TIMEOUT:-5m}"
wait_seconds="${KIND_WAIT_SECONDS:-300}"
context="kind-${cluster}"
cert_manager_version="${CERT_MANAGER_VERSION:-v1.20.2}"
metrics_server_version="${METRICS_SERVER_VERSION:-v0.8.1}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

require go
require "${kind_bin}"
require "${kubectl_bin}"
require "${container_tool}"

cluster_exists=false
while IFS= read -r existing; do
  if [[ "${existing}" == "${cluster}" ]]; then
    cluster_exists=true
    break
  fi
done < <("${kind_bin}" get clusters)

if [[ "${cluster_exists}" == "false" ]]; then
  echo "Creating Kind cluster ${cluster}..."
  "${kind_bin}" create cluster --name "${cluster}"
else
  echo "Reusing Kind cluster ${cluster}..."
  "${kind_bin}" export kubeconfig --name "${cluster}"
fi

if ! "${kubectl_bin}" --context "${context}" get \
  crd/certificates.cert-manager.io >/dev/null 2>&1; then
  echo "Installing cert-manager ${cert_manager_version} for admission webhooks..."
  "${kubectl_bin}" --context "${context}" apply -f \
    "https://github.com/cert-manager/cert-manager/releases/download/${cert_manager_version}/cert-manager.yaml"
fi
for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
  "${kubectl_bin}" --context "${context}" wait \
    --for=condition=Available \
    "deployment/${deployment}" \
    --namespace cert-manager \
    --timeout="${timeout}"
done

if [[ "${INSTALL_METRICS_SERVER:-true}" == "true" ]]; then
  echo "Installing Metrics Server ${metrics_server_version} for HPA testing..."
  "${kubectl_bin}" --context "${context}" apply -f \
    "https://github.com/kubernetes-sigs/metrics-server/releases/download/${metrics_server_version}/components.yaml"
  metrics_args="$("${kubectl_bin}" --context "${context}" -n kube-system get \
    deployment/metrics-server \
    -o jsonpath='{.spec.template.spec.containers[0].args[*]}')"
  if [[ "${metrics_args}" != *"--kubelet-insecure-tls"* ]]; then
    "${kubectl_bin}" --context "${context}" -n kube-system patch \
      deployment/metrics-server \
      --type=json \
      -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
  fi
  "${kubectl_bin}" --context "${context}" wait \
    --for=condition=Available \
    deployment/metrics-server \
    --namespace kube-system \
    --timeout="${timeout}"
fi

architecture="$("${container_tool}" info --format '{{.Architecture}}')"
case "${architecture}" in
  amd64 | x86_64)
    go_arch=amd64
    ;;
  arm64 | aarch64)
    go_arch=arm64
    ;;
  *)
    echo "Unsupported container architecture: ${architecture}" >&2
    exit 1
    ;;
esac

echo "Building ${image} for linux/${go_arch}..."
CGO_ENABLED=0 GOOS=linux GOARCH="${go_arch}" \
  go build -trimpath -o bin/manager-kind ./cmd/main.go
"${container_tool}" build -f hack/kind.Dockerfile -t "${image}" .

echo "Loading ${image} into ${cluster}..."
"${kind_bin}" load docker-image "${image}" --name "${cluster}"

echo "Installing CRDs and controller..."
make manifests generate
"${kubectl_bin}" --context "${context}" apply -k config/default
"${kubectl_bin}" --context "${context}" wait \
  --for=condition=Established \
  crd/deployments.churnless.io \
  crd/replicasets.churnless.io \
  --timeout="${timeout}"
"${kubectl_bin}" --context "${context}" -n churnless-system set image \
  deployment/churnless-controller-manager \
  manager="${image}"
"${kubectl_bin}" --context "${context}" -n churnless-system rollout restart \
  deployment/churnless-controller-manager
"${kubectl_bin}" --context "${context}" -n churnless-system rollout status \
  deployment/churnless-controller-manager \
  --timeout="${timeout}"

webhook_ready=false
for ((attempt = 0; attempt < wait_seconds; attempt++)); do
  ca_bundle="$("${kubectl_bin}" --context "${context}" get \
    validatingwebhookconfiguration/churnless-validating-webhook-configuration \
    -o jsonpath='{.webhooks[0].clientConfig.caBundle}' 2>/dev/null || true)"
  if [[ -n "${ca_bundle}" ]]; then
    webhook_ready=true
    break
  fi
  sleep 1
done
if [[ "${webhook_ready}" == "false" ]]; then
  echo "Timed out waiting for the admission webhook CA bundle" >&2
  exit 1
fi

echo "Deploying the sample custom Deployment..."
"${kubectl_bin}" --context "${context}" apply \
  -f config/samples/churnless_v1alpha1_deployment.yaml
pods_exist=false
for ((attempt = 0; attempt < wait_seconds; attempt++)); do
  pod_count="$("${kubectl_bin}" --context "${context}" get pods \
    -l app=deployment-sample \
    -o jsonpath='{.items[*].metadata.name}' 2>/dev/null | wc -w | tr -d ' ')"
  if [[ "${pod_count}" == "2" ]]; then
    pods_exist=true
    break
  fi
  sleep 1
done
if [[ "${pods_exist}" == "false" ]]; then
  echo "Timed out waiting for deployment-sample Pods" >&2
  exit 1
fi
"${kubectl_bin}" --context "${context}" wait \
  --for=condition=Ready \
  pod \
  -l app=deployment-sample \
  --timeout="${timeout}"

echo
echo "Churnless is ready in kubectl context ${context}."
echo
"${kubectl_bin}" --context "${context}" get \
  deployment.churnless.io/deployment-sample
"${kubectl_bin}" --context "${context}" get \
  replicasets.churnless.io \
  -l churnless.io/structural-revision
"${kubectl_bin}" --context "${context}" get \
  pods \
  -l app=deployment-sample \
  -o wide
echo
echo "Run 'make kind-status' to inspect it and 'make kind-down' when finished."
