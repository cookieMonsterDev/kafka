#!/usr/bin/env bash
set -euo pipefail

# Honors KAFKA_VERSION (default 4.0), COMPOSE_FILE, and OAUTHBEARER_ENABLED.
# Set KAFKA_EXTERNAL=1 to skip compose from Vitest global-setup instead of this script.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

echo "Starting cluster from ${COMPOSE_FILE} (KAFKA_VERSION=${KAFKA_VERSION:-4.0})"
docker compose -f "${COMPOSE_FILE}" up --wait --wait-timeout 180

# ZooKeeper images cannot bake SCRAM into kafka-storage format; register after boot.
if [[ "${COMPOSE_FILE}" == *zk-* ]]; then
  "${ROOT}/scripts/create-scram-credentials.sh"
fi
