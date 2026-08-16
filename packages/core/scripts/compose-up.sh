#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

echo "Starting cluster from ${COMPOSE_FILE}"
docker compose -f "${COMPOSE_FILE}" up --wait --wait-timeout 180

# ZooKeeper images cannot bake SCRAM into kafka-storage format; register after boot.
if [[ "${COMPOSE_FILE}" == *zk-* ]]; then
  "${ROOT}/scripts/create-scram-credentials.sh"
fi
