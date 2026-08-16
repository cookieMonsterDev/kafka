#!/usr/bin/env bash
# Register the integration-test SCRAM user on a ZooKeeper Kafka cluster.
# KRaft stacks bake these credentials into kafka-storage.sh format instead.
set -euo pipefail

USERNAME="${SCRAM_USERNAME:-testscram}"
PASSWORD_256="${SCRAM_PASSWORD_256:-testtestscram=256}"
PASSWORD_512="${SCRAM_PASSWORD_512:-testtestscram=512}"

find_container_id() {
  docker ps \
    --filter "status=running" \
    --filter "label=custom.project=kafka-core" \
    --filter "label=custom.service=kafka1" \
    --no-trunc \
    -q
}

CONTAINER_ID="$(find_container_id)"
if [[ -z "${CONTAINER_ID}" ]]; then
  echo "No running kafka-core kafka1 container; skip SCRAM bootstrap." >&2
  exit 1
fi

echo "Registering SCRAM user '${USERNAME}' on ${CONTAINER_ID}"
docker exec "${CONTAINER_ID}" \
  kafka-configs --zookeeper zookeeper:2181 --alter \
  --add-config "SCRAM-SHA-256=[iterations=8192,password=${PASSWORD_256}],SCRAM-SHA-512=[password=${PASSWORD_512}]" \
  --entity-type users --entity-name "${USERNAME}"
