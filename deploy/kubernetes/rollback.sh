#!/usr/bin/env bash
set -Eeuo pipefail

namespace="${VAYOBYZH_NAMESPACE:-vayobyzh}"
release="${VAYOBYZH_RELEASE:-vayobyzh}"
revision="${1:?Usage: ./deploy/kubernetes/rollback.sh HELM_REVISION}"

helm rollback "${release}" "${revision}" -n "${namespace}" --wait --timeout 10m
kubectl rollout status deployment \
  -n "${namespace}" \
  -l app.kubernetes.io/part-of=vayobyzh \
  --timeout=10m
