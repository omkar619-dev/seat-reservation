#!/usr/bin/env bash
# On-sale stampede + correctness checks against a running service.
#
#   ./burst.sh <BASE_URL> [burst flags...]
#   ./burst.sh https://<your-app>.up.railway.app -concurrency 500
#   ADMIN_KEY=... ./burst.sh https://...      (admin key creates the fresh show)
#
# Uses Go if installed, otherwise builds and runs the burst image with Docker.
set -euo pipefail
cd "$(dirname "$0")"

BASE_URL="${1:-http://localhost:8080}"
shift || true

if command -v go >/dev/null 2>&1; then
  exec go run ./cmd/burst "$@" "$BASE_URL"
fi

docker build -q --target burst -t seat-reservation-burst . >/dev/null
# localhost inside the container is the container itself; reach the host's port instead.
# --add-host makes host.docker.internal resolve on Linux Docker Engine too (Docker Desktop has it).
TARGET="${BASE_URL/localhost/host.docker.internal}"
TARGET="${TARGET/127.0.0.1/host.docker.internal}"
exec docker run --rm --add-host=host.docker.internal:host-gateway -e ADMIN_KEY \
  seat-reservation-burst "$@" "$TARGET"
