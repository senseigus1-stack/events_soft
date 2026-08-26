#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_dir}"
docker compose -f docker-compose.yml -f docker-compose.prod.yml down
