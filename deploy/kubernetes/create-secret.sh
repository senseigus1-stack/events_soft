#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_file="${1:-${repo_dir}/.env.kubernetes}"
namespace="${VAYOBYZH_NAMESPACE:-vayobyzh}"
secret_name="${VAYOBYZH_SECRET_NAME:-vayobyzh-secrets}"

if [[ ! -f "${env_file}" ]]; then
  echo "Missing ${env_file}. Copy .env.kubernetes.example and fill it in."
  exit 1
fi
if grep -q 'CHANGE_ME' "${env_file}"; then
  echo "Replace every CHANGE_ME value in ${env_file}."
  exit 1
fi

kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${namespace}" create secret generic "${secret_name}" \
  --from-env-file="${env_file}" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "Secret ${namespace}/${secret_name} applied."
