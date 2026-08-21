---
title: Compatibility
description: Broker versions, Java-client defaults, and APIs this client does not implement
order: 8
section: reference
---

`@cookiemonsterdev/kafka-core` is a TypeScript producer/consumer/admin client. It is **not** a
Java-client equivalent and does not claim full Kafka 4.x Java-client parity.

The client negotiates protocol versions from `ApiVersions`. The support floor
is **Kafka 0.10+**. Official Apache Kafka compatibility tables describe Java
client jars; this library talks to brokers through overlapping protocol
versions instead. See
[Apache Kafka compatibility](https://kafka.apache.org/43/getting-started/compatibility/)
and the [protocol guide](https://kafka.apache.org/43/design/protocol/).

## Tested brokers

Integration tests cover **0.10, 0.11, 1.1, 2.4, 3.6, 4.0, and 4.3**.
Default `KAFKA_VERSION` remains **4.0**. Compose files also exist for 4.1 and
4.2 (`apache/kafka:4.1.2` / `4.2.1` / `4.3.1`); CI PRs run 4.3 and default-branch
pushes run the full matrix.

The client talks to **4.0 and 4.3** via overlap (Produce 3–13, Fetch 4–12,
Metadata 0–13). Kafka 4.0 brokers dropped Produce v0–2 and Fetch v0–3 (KIP-896;
see
[Apache Kafka compatibility](https://kafka.apache.org/43/getting-started/compatibility/));
the client still encodes those versions for 0.10 clusters and will not send
them to 4.0 because the broker does not advertise them. Kafka 4.0 advertises
Produce through v12; v13 (topic IDs, KIP-516) is used when the broker
advertises it and Cluster metadata includes a `topicId`.

Metadata v10–v13 decode KIP-516 topic IDs (`topicId` as a 16-byte `Buffer` on
each topic). Produce v13 addresses topics by those IDs; earlier Produce
versions and Fetch still use topic names.

| `KAFKA_VERSION`                | Status                                      |
| ------------------------------ | ------------------------------------------- |
| 0.10, 0.11, 1.1, 2.4, 3.6, 4.0 | Covered by integration tests                |
| 4.1, 4.2                       | Compose files in tree; default-branch CI    |
| 4.3                            | Covered by integration tests; CI PRs run it |

## Defaults vs the Java client

These defaults are kept on purpose. They are **not** the Java 4.3 defaults.

| Setting              | Java 4.3                                  | This client                                                                                |
| -------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------ |
| `enable.idempotence` | `true` (since 3.0)                        | `idempotent: false`                                                                        |
| `isolation.level`    | `read_uncommitted`                        | `read_committed` (`readUncommitted: false`)                                                |
| `linger.ms`          | 5 ms (since 4.0); the Java client batches | `lingerMs` defaults to 0 (one Produce per `send()`); set `lingerMs` / `batchSize` to batch |
| Partitioner          | Sticky until `batch.size` (4.x)           | murmur2 by default; KIP-794 `Partitioners.StickyPartitioner` is opt-in                     |
| Compression          | gzip, snappy, lz4, zstd                   | GZIP and ZSTD are built in; Snappy and LZ4 are pluggable stubs                             |

See [producer configs](https://kafka.apache.org/43/configuration/producer-configs/)
and [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).

The opt-in sticky partitioner keeps unkeyed records on one partition for each
Produce batch formed by this client's `lingerMs` / `batchSize` model, then
rotates uniformly to a different available partition. Explicit partitions and
keyed murmur2 routing are unchanged.

## Not yet at the Java 4.3 surface

**Consumer.** Range, RoundRobin, Sticky, and CooperativeSticky are built in
(`PartitionAssigners`). The default assigner is still round-robin. Groups use
the classic protocol only — there is no `group.protocol=consumer` (KIP-848; see
[consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/)).
`fromBeginning` is boolean (earliest vs latest). `autoOffsetReset: 'none'`
is supported and throws if there is no committed offset. Cooperative-sticky
uses KIP-429 incremental revoke semantics and performs the follow-up generation
needed to settle partitions that move between members. This support applies to
the classic group protocol; the KIP-848 consumer group protocol remains
unsupported.

**Admin.** `admin.alterConfigs` is kept for older brokers. Prefer
`admin.incrementalAlterConfigs` (key 44). `admin.electLeaders` is key 43
(historically ElectPreferredLeaders). `admin.deleteGroupOffsets` is OffsetDelete
(key 47). `admin.describeUserScramCredentials` and `admin.alterUserScramCredentials`
are keys 50–51. `admin.describeClientQuotas` / `admin.alterClientQuotas` are
keys 48–49. `admin.describeLogDirs` / `admin.alterReplicaLogDirs` are keys
34–35. `admin.describeCluster` uses DescribeCluster (key 60) when advertised
and Metadata otherwise. `admin.describeProducers` uses key 61 on Kafka 3.0+
and queries partition leaders unless a `brokerId` is supplied.
`admin.describeTransactions` uses key 65, dynamically discovers transaction
coordinators, and requires Kafka 3.0+. `admin.updateFeatures` implements
UpdateFeatures (key 57) v0–v2 and targets the active controller; v0 cannot
validate-only and rejects unsafe downgrades. Still missing: the remaining
transaction administration APIs.

**Security.** SASL PLAIN, SCRAM, and OAUTHBEARER are implemented. GSSAPI /
Kerberos is not. The `aws` SASL helper is extra (non-Apache). See
[SASL authentication](https://kafka.apache.org/43/security/authentication-using-sasl/).

**Out of scope.** No Kafka Streams or Kafka Connect packages. See
[Kafka Streams](https://kafka.apache.org/43/streams/introduction/) and
[Kafka Connect](https://kafka.apache.org/43/kafka-connect/overview/).

Offsets as `bigint`, MessageSet, ZSTD, and `KAFKA_*` env vars:
[Breaking changes](../migration/breaking-changes/).
