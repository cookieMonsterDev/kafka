#!/usr/bin/env bash
set -euo pipefail

# Same KAFKA_VERSION / COMPOSE_FILE / OAUTHBEARER_ENABLED mapping as compose-up.sh.
# Vitest global-setup skips teardown when KAFKA_EXTERNAL=1 or DO_NOT_STOP=1.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

docker compose -f "${COMPOSE_FILE}" down --remove-orphans
