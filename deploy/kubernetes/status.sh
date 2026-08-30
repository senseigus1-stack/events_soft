#!/usr/bin/env bash
set -Eeuo pipefail

namespace="${VAYOBYZH_NAMESPACE:-vayobyzh}"
release="${VAYOBYZH_RELEASE:-vayobyzh}"

helm status "${release}" -n "${namespace}"
kubectl get pods,svc,ingress,certificate -n "${namespace}"
kubectl get events -n "${namespace}" --sort-by=.lastTimestamp | tail -n 30
