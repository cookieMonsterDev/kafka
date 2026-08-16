---
title: Introduction
description: A TypeScript Kafka client for Kafka 0.10+
order: 1
---

`@kafka/core` is a TypeScript Kafka client:
`new Kafka({ brokers }).producer() / consumer() / admin()`.

It targets **Kafka 0.10 and newer**. Offsets are `bigint`, ZSTD is built in
when the broker supports it, and types are generated from source rather than
hand-maintained.

The client never talks to ZooKeeper. ZooKeeper is a broker deployment detail
for pre-KRaft clusters, not a client feature. See
[KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/)
and the [Apache Kafka introduction](https://kafka.apache.org/43/getting-started/introduction/).

This site documents the public API. The library lives in `packages/core`; this
Astro site is `packages/docs`.

## Supported brokers

The client negotiates protocol versions from `ApiVersions`. A broker that
advertises an API uses the highest version both sides implement. A broker that
is too old for a _used_ API throws `KafkaServerDoesNotSupportApiKey`.

| Broker        | Record format                         | Notes                                                                            |
| ------------- | ------------------------------------- | -------------------------------------------------------------------------------- |
| Kafka 0.10    | MessageSet (magic 0/1)                | No headers, no transactions                                                      |
| Kafka 0.11    | RecordBatch (magic 2)                 | Headers, transactions, ACLs, DescribeConfigs                                     |
| Kafka 1.x     | RecordBatch                           | SaslAuthenticate, CreatePartitions, DeleteGroups                                 |
| Kafka 2.x     | RecordBatch                           | ZSTD from 2.1, rack-aware fetch from 2.2                                         |
| Kafka 3.x     | RecordBatch, KRaft                    | Same client APIs as 2.x                                                          |
| Kafka 4.0     | RecordBatch, KRaft only on the broker | Talks via overlap (Produce 3–7, Fetch 4–11)                                      |
| Kafka 4.1–4.3 | RecordBatch, KRaft only on the broker | Compose files in tree (`apache/kafka:4.1.2` / `4.2.1` / `4.3.1`); CI PRs run 4.3 |

This is not Java-client 4.x parity. Integration tests historically cover
0.10, 0.11, 1.1, 2.4, 3.6, and 4.0. Kafka 4.0 brokers no longer run ZooKeeper.
The client still encodes older Produce/Fetch versions when a 0.10–3.x broker
advertises them. Defaults that differ from the Java client, and APIs that are
not implemented yet, are listed under [Compatibility](/docs/compatibility/).

## Running tests against a version

Unit tests do not start Docker. Integration tests pick a compose file from
`KAFKA_VERSION` (default `4.0`):

```sh
pnpm --filter @kafka/core test
KAFKA_VERSION=0.10 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.0 pnpm --filter @kafka/core test:integration
KAFKA_VERSION=4.3 pnpm --filter @kafka/core test:integration
```

Leave a cluster running with `DO_NOT_STOP=1`, or point at an already-running
cluster with `KAFKA_EXTERNAL=1`. The mapping lives in
`packages/core/test/assets/README.md`.
