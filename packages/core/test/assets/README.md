# Integration cluster assets

Integration tests start a local Kafka cluster with Docker Compose. Select the stack with `KAFKA_VERSION` (semver, default `4.0`) and optionally `OAUTHBEARER_ENABLED=1`.

```bash
KAFKA_VERSION=0.11 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=1.1 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=2.4 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=3.6 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.0 pnpm --filter @kafka/core test:integration
```

`global-setup.ts`, `scripts/compose-up.sh`, and `scripts/compose-down.sh` all resolve the same mapping. `COMPOSE_FILE` overrides it when you need a one-off stack.

## Version → compose file

| `KAFKA_VERSION` | Compose file                   | Mode  | Status    |
| --------------- | ------------------------------ | ----- | --------- |
| `0.10`          | `docker-compose.zk-0-10.yml`   | ZK    | planned   |
| `0.11`          | `docker-compose.zk-0-11.yml`   | ZK    | available |
| `1.1`           | `docker-compose.zk-1-1.yml`    | ZK    | available |
| `2.4`           | `docker-compose.zk-2-4.yml`    | ZK    | available |
| `3.6`           | `docker-compose.kraft-3-6.yml` | KRaft | available |
| `4.0` (default) | `docker-compose.kraft.yml`     | KRaft | available |

`KAFKA_VERSION=0.11` uses `confluentinc/cp-kafka:3.3.3` (Kafka 0.11) plus ZooKeeper. Produce negotiates v3 and Fetch v5 (RecordBatch, headers, transactions). There is no `SaslAuthenticate` API — SASL uses handshake + raw bytes. CreatePartitions, DeleteGroups, and DescribeConfigs synonyms are absent. CreateTopics without an explicit `numPartitions` sends `-1` (broker default), which this broker rejects; tests pass a positive partition count.

`KAFKA_VERSION=1.1` uses `confluentinc/cp-kafka:4.1.3` (Kafka 1.1). Produce negotiates v5 and Fetch v7 (incremental fetch sessions). SaslAuthenticate v0, CreatePartitions, DeleteGroups, and DescribeConfigs v1 synonyms are available. SaslAuthenticate `sessionLifetimeMs` arrives later (v1 / Kafka 2.0).

`KAFKA_VERSION=2.4` uses `confluentinc/cp-kafka:5.4.2` (Kafka 2.4) plus ZooKeeper. The image is amd64-only; Compose sets `platform: linux/amd64`. SCRAM users are registered after boot by `scripts/create-scram-credentials.sh`. SSL uses `certs/kafka.server.*-java8.p12` (OpenSSL 3 `-legacy`) because Java 8 cannot load the KRaft PKCS12 files. The authorizer is `kafka.security.auth.SimpleAclAuthorizer`. The 0.11 and 1.1 stacks share that cert/JAAS/authorizer setup.

`KAFKA_VERSION=3.6` uses `apache/kafka:3.9.1`. Official `apache/kafka` images start at 3.7.0; 3.7.2 omits SCRAM credentials from `kafka-storage.sh format` ([KAFKA-17636](https://issues.apache.org/jira/browse/KAFKA-17636)). Combined broker/controller mode matches the 4.0 stack (`apache/kafka:4.0.0`).

OAUTHBEARER cannot share listeners with PLAIN/SCRAM. When `OAUTHBEARER_ENABLED=1`, the runner uses `docker-compose.kraft-oauthbearer.yml` (Kafka 4.0) regardless of `KAFKA_VERSION`.

`docker-compose.kraft-sasl.yml` is included by the KRaft files for SASL listeners; do not pass it as `KAFKA_VERSION`.

## Feature gates

Suites that need a minimum (or maximum) broker version should import the helpers from `test/helpers/index.ts`:

- `testIfKafkaAtMost_0_10` / `_0_11` / `_1_1` / `_3_6`
- `testIfKafkaAtLeast_0_11` / `_1_0` / `_1_1` / `_2_1` / `_2_2` / `_2_4` / `_3_0` / `_3_6` / `_4_0`
- `testIfKafkaEquals_0_11` / `_1_1`
- `describeIfKRaft` / `describeIfZooKeeper`

These read `KAFKA_VERSION` (default `4.0`) and skip when the running cluster is outside the range. Prefer them over parsing the env var in individual tests.

## Other env vars

| Variable           | Effect                                           |
| ------------------ | ------------------------------------------------ |
| `KAFKA_EXTERNAL=1` | Skip compose up/down (cluster already running)   |
| `DO_NOT_STOP=1`    | Leave the cluster running after tests            |
| `TEST_RETRIES`     | Vitest retry count for the integration project   |
| `COMPOSE_FILE`     | Absolute or relative path; skips version mapping |
