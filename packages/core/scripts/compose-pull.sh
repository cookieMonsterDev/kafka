#!/usr/bin/env bash
set -euo pipefail

# Honors KAFKA_VERSION, COMPOSE_FILE, and OAUTHBEARER_ENABLED via resolve-compose-file.sh.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

echo "Pulling images for ${COMPOSE_FILE} (KAFKA_VERSION=${KAFKA_VERSION:-4.0})"
docker compose -f "${COMPOSE_FILE}" pull
