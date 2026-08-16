#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$("${ROOT}/scripts/resolve-compose-file.sh")"

docker compose -f "${COMPOSE_FILE}" down --remove-orphans
