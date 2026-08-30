#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_file="${VAYOBYZH_ENV_FILE:-${repo_dir}/.env.kubernetes}"
namespace="${VAYOBYZH_NAMESPACE:-vayobyzh}"
release="${VAYOBYZH_RELEASE:-vayobyzh}"
image_tag="${VAYOBYZH_IMAGE_TAG:-rc-1.0.3}"
cert_manager_version="${CERT_MANAGER_VERSION:-v1.21.1}"

read_env() {
  local key="$1"
  awk -F= -v key="${key}" '$1 == key {sub(/^[^=]*=/, ""); value=$0} END {gsub(/\r$/, "", value); print value}' "${env_file}"
}

for command_name in kubectl helm; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing ${command_name}. Run sudo ./deploy/kubernetes/install-k3s.sh first."
    exit 1
  fi
done
if [[ ! -f "${env_file}" ]]; then
  echo "Missing ${env_file}. Copy .env.kubernetes.example and fill it in."
  exit 1
fi

domain="$(read_env DOMAIN)"
tls_email="$(read_env TLS_EMAIL)"
if [[ -z "${domain}" || -z "${tls_email}" ]]; then
  echo "DOMAIN and TLS_EMAIL are required in ${env_file}."
  exit 1
fi

"${repo_dir}/deploy/kubernetes/create-secret.sh" "${env_file}"

helm upgrade --install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version "${cert_manager_version}" \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true \
  --atomic \
  --timeout 10m

helm upgrade --install "${release}" "${repo_dir}/deploy/helm/vayobyzh" \
  --namespace "${namespace}" \
  --create-namespace \
  --values "${repo_dir}/deploy/kubernetes/values.single-node.yaml" \
  --set-string images.api.tag="${image_tag}" \
  --set-string images.web.tag="${image_tag}" \
  --set-string ingress.host="${domain}" \
  --set-string ingress.tls.secretName="${release}-tls" \
  --set-string certManager.email="${tls_email}" \
  --set-string config.CORS_ORIGINS="https://${domain}" \
  --set-string config.OAUTH_PUBLIC_BASE_URL="https://${domain}" \
  --set-string config.OAUTH_FRONTEND_URL="https://${domain}" \
  --atomic \
  --timeout 15m

kubectl -n "${namespace}" get pods,svc,ingress
echo "Deployment completed: https://${domain}"
