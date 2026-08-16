#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

echo "Starting cluster from ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" up --wait --wait-timeout 120
