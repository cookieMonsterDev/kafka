#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT}/test/assets/docker-compose.kraft.yml}"

if [[ "${OAUTHBEARER_ENABLED:-}" == "1" ]]; then
  COMPOSE_FILE="${ROOT}/test/assets/docker-compose.kraft-oauthbearer.yml"
fi

docker compose -f "${COMPOSE_FILE}" down --remove-orphans
