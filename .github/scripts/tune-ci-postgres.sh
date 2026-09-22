#!/usr/bin/env bash
set -euo pipefail

admin_url="${E2E_PG_ADMIN_URL:-${DATABASE_URL:?E2E_PG_ADMIN_URL or DATABASE_URL must be set}}"

ready=0
for _ in $(seq 1 30); do
  if psql "$admin_url" -tAc 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "postgres did not accept a connection within 30s" >&2
  exit 1
fi

psql "$admin_url" -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET fsync = off" \
  -c "ALTER SYSTEM SET full_page_writes = off" \
  -c "ALTER SYSTEM SET synchronous_commit = off" \
  -c "SELECT pg_reload_conf()"

applied="$(psql "$admin_url" -tAc 'SHOW fsync')"
echo "fsync is now: ${applied}"
if [ "$applied" != "off" ]; then
  echo "the reload did not take effect (fsync still ${applied})" >&2
  exit 1
fi
