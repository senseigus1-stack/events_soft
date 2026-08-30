#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_dir}"
git pull --ff-only origin rc-1.0.3
./deploy/ubuntu/up.sh
