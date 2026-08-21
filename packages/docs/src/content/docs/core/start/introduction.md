---
title: Introduction
description: A TypeScript Kafka client for Kafka 0.10+
order: 1
section: start
---

`@cookiemonsterdev/kafka-core` is a TypeScript Kafka client:
`new Kafka({ brokers }).producer() / consumer() / admin()`.

It targets **Kafka 0.10 and newer**. Offsets are `bigint`, ZSTD is built in
when the broker supports it, and types are generated from source rather than
hand-maintained.

The client never talks to ZooKeeper. ZooKeeper is a broker deployment detail
for pre-KRaft clusters, not a client feature. See
[KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/)
and the [Apache Kafka introduction](https://kafka.apache.org/43/getting-started/introduction/).

This site documents the public API. The library lives in
[`packages/core`](https://github.com/cookieMonsterDev/kafka/tree/develop/packages/core);
this Astro site is `packages/docs`.

Next: [Installation](./installation/),
[Getting started](./getting-started/),
[Kafka client](../reference/kafka/),
[Errors](../guides/errors/),
[Breaking changes](../migration/breaking-changes/).

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
| Kafka 4.0     | RecordBatch, KRaft only on the broker | Talks via overlap (Produce 3–10, Fetch 4–18)                                     |
| Kafka 4.1–4.3 | RecordBatch, KRaft only on the broker | Compose files in tree (`apache/kafka:4.1.2` / `4.2.1` / `4.3.1`); CI PRs run 4.3 |

This is not Java-client 4.x parity. Integration tests cover 0.10, 0.11, 1.1,
2.4, 3.6, 4.0, and 4.3. Kafka 4.0 brokers no longer run ZooKeeper.
The client still encodes older Produce/Fetch versions when a 0.10–3.x broker
advertises them. Defaults that differ from the Java client, and APIs that are
not implemented yet, are listed under [Compatibility](../reference/compatibility/).
How to run the integration matrix: [Testing](../guides/testing/).
