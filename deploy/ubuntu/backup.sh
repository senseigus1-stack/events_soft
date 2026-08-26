#!/usr/bin/env bash
set -Eeuo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
backup_dir="${VAYOBYZH_BACKUP_DIR:-${repo_dir}/backups}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "${backup_dir}"
cd "${repo_dir}"

docker compose exec -T db sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "${backup_dir}/vayobyzh-${timestamp}.dump"
find "${backup_dir}" -type f -name 'vayobyzh-*.dump' -mtime +14 -delete
echo "Backup created: ${backup_dir}/vayobyzh-${timestamp}.dump"
