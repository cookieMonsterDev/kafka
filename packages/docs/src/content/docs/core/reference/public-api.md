---
title: Public API
description: What @cookiemonsterdev/kafka-core exports
order: 7
section: reference
---

The public barrel is
[`src/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/index.ts).
Types are generated from the TypeScript source (`tsc --emitDeclarationOnly`);
there is no hand-maintained `types/index.d.ts`.

## Runtime exports

| Export                                                                                | Kind                                                                                            | Page                                                       |
| ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `Kafka`                                                                               | `producer()` / `consumer()` / `shareConsumer()` / `admin()` / `logger()`                        | [Kafka client](./kafka/)                                   |
| `throughputPreset`                                                                    | sticky producer + consume-concurrency profile (linger/batch/in-flight are constructor defaults) | [Throughput](../../guides/throughput/)                     |
| `Partitioners`                                                                        | `DefaultPartitioner` (murmur2 keyed routing), `LegacyPartitioner`, `StickyPartitioner`          | [Producer](../../guides/producer/#partitioners)            |
| `PartitionAssigners`                                                                  | `roundRobin`, `range`, `sticky`, `cooperativeSticky`                                            | [Consumer](../../guides/consumer/#assigners-and-isolation) |
| `AssignerProtocol`                                                                    | `MemberMetadata`, `MemberAssignment` encode/decode                                              |                                                            |
| `logLevel`                                                                            | `NOTHING`, `ERROR`, `WARN`, `INFO`, `DEBUG`                                                     | [Configuration](./configuration/)                          |
| `CompressionTypes`                                                                    | `None`, `GZIP`, `Snappy`, `LZ4`, `ZSTD`                                                         |                                                            |
| `CompressionCodecs`                                                                   | Codec registry; GZIP, Snappy, LZ4, and ZSTD built in                                            |                                                            |
| `ConfigResourceTypes`, `ConfigSource`, `ConfigOperations`, `ConfigType`               | Config APIs                                                                                     | [Admin](./admin/)                                          |
| `AclResourceTypes`, `AclOperationTypes`, `AclPermissionTypes`, `ResourcePatternTypes` | ACL APIs                                                                                        | [Admin](./admin/)                                          |
| `ScramMechanisms`                                                                     | SCRAM                                                                                           | [Admin](./admin/)                                          |
| `FeatureUpdateUpgradeTypes`                                                           | `UPGRADE`, `SAFE_DOWNGRADE`, `UNSAFE_DOWNGRADE`                                                 | [Admin](./admin/)                                          |
| `SHARE_ACKNOWLEDGE_TYPE`, `SHARE_ACQUIRE_MODE`                                        | Share-group ack types and acquire modes                                                         | [Consumer](./consumer/#shareconsumer)                      |
| `Kafka*` error classes                                                                | See [Errors](./errors/)                                                                         |                                                            |
| `METRIC_NAMES`                                                                        | OpenTelemetry / KIP-714 metric name constants                                                   | [Observability](../../guides/observability/)               |

`isRebalancing` and `isKafkaError` are not part of the public barrel.
Snappy uses xerial snappy-java framing; LZ4 uses the LZ4 Frame format (LZ4F).

## Types

Exported from the barrel: `KafkaConfig`, `ProducerConfig`, `ConsumerConfig`,
`ShareConsumerConfig`, `AdminConfig`, `Producer`, `Consumer`, `ShareConsumer`,
`Admin`, `Transaction`, `Message`,
`KafkaMessage`, `ProducerRecord`, `EachMessagePayload`, `EachBatchPayload`,
`GroupProtocol`, `GroupRemoteAssignor`, `ShareAcknowledgeType`, `ShareAcquireMode`, `DescribeProducersOptions`, `PartitionProducerState`, `ActiveProducerState`,
`ListTransactionsOptions`, `TransactionListing`, `TransactionDescription`,
`KafkaPrincipal`, `CreateDelegationTokenResult`, `DelegationToken`,
`KafkaMetrics`, `KafkaMeter`, `METRIC_NAMES`,
and SASL types including `GssTokenProvider` / `GssTokenChallenge` /
`GssTokenStep`. Field-by-field:
[Configuration](./configuration/),
[Producer API](./producer/),
[Consumer API](./consumer/),
[Admin API](./admin/),
[Security](../../guides/security/).

## Extra APIs

- `AbortSignal` on `connect` / `disconnect` / `send` / `sendBatch` / `run`
- `consumer.stream()` — async iteration over batches
- `consumer.assign()` — fetch exact partitions with no group membership; mutually exclusive with
  `subscribe()` (see [Consumer: Assign mode](../../guides/consumer/#assign-mode))
- `Symbol.asyncDispose` on producer, consumer, share consumer, and admin (`await using`)
- Built-in Snappy (`CompressionTypes.Snappy`), LZ4 (`CompressionTypes.LZ4`), and ZSTD (`CompressionTypes.ZSTD`)
- Optional OpenTelemetry metrics via `KafkaConfig.metrics` (see [Observability](../../guides/observability/))
- Admin `bootstrapControllers` for KRaft controller-only discovery (KIP-919)
- `producer.listTopics()` / `consumer.listTopics()` and `partitionsFor(topic)` over cluster metadata
- Connection knobs: `connectionsMaxIdleMs`, `clientDnsLookup`, `reconnectBackoffMs` / max, `socketConnectionSetupTimeoutMaxMs`
- KIP-714 client telemetry (`enableMetricsPush`, `clientInstanceId()`) — [Observability](../../guides/observability/)

## Capability errors

The client checks broker `ApiVersions` rather than a version string. Missing
or too-old APIs fail fast with a non-retriable error.

| Situation                                         | Error                                                                                                      |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Headers on a MessageSet broker (Kafka 0.10)       | `Message headers require Produce API version 3 or higher (Kafka 0.11+)`                                    |
| `idempotent: true` or `transactionalId` on 0.10   | `Idempotent and transactional producers require InitProducerId (Kafka 0.11+)`                              |
| `CompressionTypes.ZSTD` before Produce v7         | `ZSTD compression requires Produce API version 7 or higher (Kafka 2.1+)`                                   |
| `ResourcePatternTypes.PREFIXED` on ACL APIs v0    | `Prefixed ACL resource patterns require ACL APIs v1 (Kafka 2.0+); this broker negotiated v0`               |
| `createTopics({ validateOnly: true })` on v0      | `CreateTopics v0 does not support validateOnly; this broker needs Kafka 0.11+ (CreateTopics v1)`           |
| `listConfigResources({ resourceTypes })` on v0    | `ListConfigResources v0 does not support resourceTypes; this broker needs ListConfigResources v1 or newer` |
| Broker never advertised a used API, or no overlap | `KafkaServerDoesNotSupportApiKey`                                                                          |

`describeConfigs({ includeSynonyms: true })` is ignored on DescribeConfigs v0
(Kafka 0.11); decoded entries have an empty `configSynonyms` array.

See [Errors](./errors/) and
[Compatibility](./compatibility/).
