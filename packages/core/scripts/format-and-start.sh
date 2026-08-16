#!/usr/bin/env bash
# Wraps the official apache/kafka image startup so storage is formatted with
# SCRAM credentials. KafkaDockerWrapper.setup formats without --add-scram, so
# we let it generate server.properties against a throwaway log dir, then format
# the real log dir ourselves.

set -euo pipefail

. /etc/kafka/docker/bash-config

echo "===> User"
id

echo "===> Setting default values of environment variables if not already set."
. /etc/kafka/docker/configureDefaults

echo "===> Configuring ..."
. /etc/kafka/docker/configure

if [ -z "${KAFKA_JMX_OPTS-}" ]; then
  export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true \
    -Dcom.sun.management.jmxremote.authenticate=false \
    -Dcom.sun.management.jmxremote.ssl=false "
fi

export KAFKA_JMX_HOSTNAME=${KAFKA_JMX_HOSTNAME:-$(hostname -i | cut -d" " -f1)}

if [ "${KAFKA_JMX_PORT-}" ]; then
  export JMX_PORT=$KAFKA_JMX_PORT
  export KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS-} -Djava.rmi.server.hostname=$KAFKA_JMX_HOSTNAME \
    -Dcom.sun.management.jmxremote.local.only=false \
    -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT \
    -Dcom.sun.management.jmxremote.port=$JMX_PORT"
fi

REAL_LOG_DIRS="${KAFKA_LOG_DIRS:-/tmp/kraft-combined-logs}"
BOOTSTRAP_LOG_DIRS="/tmp/kafka-docker-bootstrap"

echo "===> Generating server.properties via KafkaDockerWrapper (throwaway log dir) ..."
export KAFKA_LOG_DIRS="${BOOTSTRAP_LOG_DIRS}"

if [ -z "${KAFKA_JVM_PERFORMANCE_OPTS-}" ]; then
  export TEMP_KAFKA_JVM_PERFORMANCE_OPTS=""
else
  export TEMP_KAFKA_JVM_PERFORMANCE_OPTS="$KAFKA_JVM_PERFORMANCE_OPTS"
fi

export KAFKA_JVM_PERFORMANCE_OPTS="${KAFKA_JVM_PERFORMANCE_OPTS-} -XX:SharedArchiveFile=/opt/kafka/storage.jsa"

result=$(/opt/kafka/bin/kafka-run-class.sh kafka.docker.KafkaDockerWrapper setup \
  --default-configs-dir /etc/kafka/docker \
  --mounted-configs-dir /mnt/shared/config \
  --final-configs-dir /opt/kafka/config 2>&1) || \
  echo "$result" | grep -i "already formatted" || \
  { echo "$result" && exit 1; }

export KAFKA_JVM_PERFORMANCE_OPTS="$TEMP_KAFKA_JVM_PERFORMANCE_OPTS"

# Point the generated config at the real log dir before we format it with SCRAM.
sed -i "s|${BOOTSTRAP_LOG_DIRS}|${REAL_LOG_DIRS}|g" /opt/kafka/config/server.properties
export KAFKA_LOG_DIRS="${REAL_LOG_DIRS}"

echo "===> Formatting ${REAL_LOG_DIRS} with SCRAM credentials ..."
# shellcheck disable=SC2016
/opt/kafka/bin/kafka-storage.sh format \
  --ignore-formatted \
  --cluster-id "${CLUSTER_ID}" \
  --config /opt/kafka/config/server.properties \
  --add-scram 'SCRAM-SHA-256=[name=testscram,password=testtestscram=256]' \
  --add-scram 'SCRAM-SHA-512=[name=testscram,password=testtestscram=512]'

export KAFKA_JVM_PERFORMANCE_OPTS="${KAFKA_JVM_PERFORMANCE_OPTS-} -XX:SharedArchiveFile=/opt/kafka/kafka.jsa"

echo "===> Launching Kafka ..."
exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
