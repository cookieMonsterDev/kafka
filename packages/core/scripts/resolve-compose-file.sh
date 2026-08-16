#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ASSETS="${ROOT}/test/assets"

if [[ -n "${COMPOSE_FILE:-}" ]]; then
  echo "${COMPOSE_FILE}"
  exit 0
fi

if [[ "${OAUTHBEARER_ENABLED:-}" == "1" ]]; then
  echo "${ASSETS}/docker-compose.kraft-oauthbearer.yml"
  exit 0
fi

VERSION="${KAFKA_VERSION:-4.0}"
VERSION="${VERSION#v}"

if [[ "${VERSION}" == *.* ]]; then
  MAJOR="${VERSION%%.*}"
  REST="${VERSION#*.}"
  MINOR="${REST%%.*}"
else
  MAJOR="${VERSION}"
  MINOR="0"
fi

NORMALIZED="${MAJOR}.${MINOR}"

case "${NORMALIZED}" in
  0.10) FILE="docker-compose.zk-0-10.yml" ;;
  0.11) FILE="docker-compose.zk-0-11.yml" ;;
  1.1) FILE="docker-compose.zk-1-1.yml" ;;
  2.4) FILE="docker-compose.zk-2-4.yml" ;;
  3.6) FILE="docker-compose.kraft-3-6.yml" ;;
  4.0) FILE="docker-compose.kraft.yml" ;;
  *)
    echo "Unsupported KAFKA_VERSION=${NORMALIZED}. Known versions: 0.10, 0.11, 1.1, 2.4, 3.6, 4.0" >&2
    exit 1
    ;;
esac

PATH_FILE="${ASSETS}/${FILE}"
if [[ ! -f "${PATH_FILE}" ]]; then
  echo "Compose file ${FILE} for KAFKA_VERSION=${NORMALIZED} is not in test/assets yet. Default: KAFKA_VERSION=4.0." >&2
  exit 1
fi

echo "${PATH_FILE}"
