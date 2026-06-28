#!/usr/bin/env bash
# Spin a throwaway Postgres, export CHRONICLE_TEST_DB, run the given command,
# then tear the container down. Usage: scripts/with-test-db.sh go test -p 1 ./...
set -euo pipefail

name="chronicle-test-pg-$$"
port="${CHRONICLE_TEST_PORT:-5433}"

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$name" \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=chronicle_test \
  -p "${port}:5432" postgres:17 >/dev/null

# Wait for readiness (no external pg_isready needed; use the container's).
for _ in $(seq 1 90); do
  if docker exec "$name" pg_isready -U postgres -d chronicle_test >/dev/null 2>&1; then
    break
  fi
done

export CHRONICLE_TEST_DB="postgres://postgres:postgres@localhost:${port}/chronicle_test?sslmode=disable"
"$@"
