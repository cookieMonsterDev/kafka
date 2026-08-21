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

| Export                                                                                | Kind                                                                                        | Page                                                    |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `Kafka`                                                                               | `producer()` / `consumer()` / `admin()` / `logger()`                                        | [Kafka client](./kafka/)                                |
| `Partitioners`                                                                        | `DefaultPartitioner`, `LegacyPartitioner`, `JavaCompatiblePartitioner`, `StickyPartitioner` | [Producer](../guides/producer/#partitioners)            |
| `PartitionAssigners`                                                                  | `roundRobin`, `range`, `sticky`, `cooperativeSticky`                                        | [Consumer](../guides/consumer/#assigners-and-isolation) |
| `AssignerProtocol`                                                                    | `MemberMetadata`, `MemberAssignment` encode/decode                                          |                                                         |
| `logLevel`                                                                            | `NOTHING`, `ERROR`, `WARN`, `INFO`, `DEBUG`                                                 | [Configuration](./configuration/)                       |
| `CompressionTypes`                                                                    | `None`, `GZIP`, `Snappy`, `LZ4`, `ZSTD`                                                     |                                                         |
| `CompressionCodecs`                                                                   | Codec registry; GZIP, Snappy, and ZSTD built in                                             |                                                         |
| `ConfigResourceTypes`, `ConfigSource`, `ConfigOperations`, `ConfigType`               | Config APIs                                                                                 | [Admin](./admin/)                                       |
| `AclResourceTypes`, `AclOperationTypes`, `AclPermissionTypes`, `ResourcePatternTypes` | ACL APIs                                                                                    | [Admin](./admin/)                                       |
| `ScramMechanisms`                                                                     | SCRAM                                                                                       | [Admin](./admin/)                                       |
| `Kafka*` error classes                                                                | See [Errors](./errors/)                                                                     |                                                         |

`isRebalancing` and `isKafkaError` are not part of the public barrel.
LZ4 is a pluggable stub, not built in. Snappy is built in (xerial snappy-java framing).

## Types

Exported from the barrel: `KafkaConfig`, `ProducerConfig`, `ConsumerConfig`,
`AdminConfig`, `Producer`, `Consumer`, `Admin`, `Transaction`, `Message`,
`KafkaMessage`, `ProducerRecord`, `EachMessagePayload`, `EachBatchPayload`,
`DescribeProducersOptions`, `PartitionProducerState`, `ActiveProducerState`,
`ListTransactionsOptions`, `TransactionListing`, `TransactionDescription`,
and SASL types. Field-by-field:
[Configuration](./configuration/),
[Producer API](./producer/),
[Consumer API](./consumer/),
[Admin API](./admin/).

## Extra APIs

- `AbortSignal` on `connect` / `disconnect` / `send` / `sendBatch` / `run`
- `consumer.stream()` — async iteration over batches
- `Symbol.asyncDispose` on producer, consumer, and admin (`await using`)
- Built-in ZSTD (`CompressionTypes.ZSTD`) and Snappy (`CompressionTypes.Snappy`)

## Capability errors

The client checks broker `ApiVersions` rather than a version string. Missing
or too-old APIs fail fast with a non-retriable error.

| Situation                                         | Error                                                                                            |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Headers on a MessageSet broker (Kafka 0.10)       | `Message headers require Produce API version 3 or higher (Kafka 0.11+)`                          |
| `idempotent: true` or `transactionalId` on 0.10   | `Idempotent and transactional producers require InitProducerId (Kafka 0.11+)`                    |
| `CompressionTypes.ZSTD` before Produce v7         | `ZSTD compression requires Produce API version 7 or higher (Kafka 2.1+)`                         |
| `ResourcePatternTypes.PREFIXED` on ACL APIs v0    | `Prefixed ACL resource patterns require ACL APIs v1 (Kafka 2.0+); this broker negotiated v0`     |
| `createTopics({ validateOnly: true })` on v0      | `CreateTopics v0 does not support validateOnly; this broker needs Kafka 0.11+ (CreateTopics v1)` |
| Broker never advertised a used API, or no overlap | `KafkaServerDoesNotSupportApiKey`                                                                |

`describeConfigs({ includeSynonyms: true })` is ignored on DescribeConfigs v0
(Kafka 0.11); decoded entries have an empty `configSynonyms` array.

See [Errors](./errors/) and
[Compatibility](./compatibility/).
