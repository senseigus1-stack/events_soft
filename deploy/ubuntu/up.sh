#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_dir}"

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.production.example to .env and fill it in."
  exit 1
fi
if grep -q 'CHANGE_ME' .env; then
  echo "Replace every CHANGE_ME value in .env before production startup."
  exit 1
fi

compose_args=(-f docker-compose.yml -f docker-compose.prod.yml)
telegram_token="$(awk -F= '$1 == "TELEGRAM_TOKEN" {sub(/^[^=]*=/, ""); value=$0} END {gsub(/\r$/, "", value); print value}' .env)"
if [[ -n "${telegram_token}" ]]; then
  compose_args+=(--profile telegram)
fi

docker compose "${compose_args[@]}" config --quiet
docker compose "${compose_args[@]}" up -d --build --remove-orphans
docker compose "${compose_args[@]}" ps
