---
title: Public API
description: What @kafka/core exports
order: 3
---

The public barrel is `src/index.ts`. Types are generated from the TypeScript
source (`tsc --emitDeclarationOnly`); there is no hand-maintained
`types/index.d.ts`.

## Runtime exports

| Export                                                                                | Kind                                                                   |
| ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `Kafka`                                                                               | Client class: `producer()`, `consumer()`, `admin()`, `logger()`        |
| `Partitioners`                                                                        | `DefaultPartitioner`, `LegacyPartitioner`, `JavaCompatiblePartitioner` |
| `PartitionAssigners`                                                                  | `{ roundRobin }`                                                       |
| `AssignerProtocol`                                                                    | `{ MemberMetadata, MemberAssignment }` encode/decode                   |
| `logLevel`                                                                            | `NOTHING`, `ERROR`, `WARN`, `INFO`, `DEBUG`                            |
| `CompressionTypes`                                                                    | `None`, `GZIP`, `Snappy`, `LZ4`, `ZSTD`                                |
| `CompressionCodecs`                                                                   | Mutable codec registry; GZIP and ZSTD are built in                     |
| `ConfigResourceTypes`, `ConfigSource`                                                 | Config APIs                                                            |
| `AclResourceTypes`, `AclOperationTypes`, `AclPermissionTypes`, `ResourcePatternTypes` | ACL APIs                                                               |
| `Kafka*` error classes                                                                | `KafkaError`, `KafkaNonRetriableError`, `KafkaProtocolError`, …        |

`isRebalancing` and `isKafkaError` are not part of the public barrel.

## Client config

```ts
new Kafka({
  brokers: ['localhost:9092'], // or () => string[] | Promise<string[]>
  ssl: true, // or a tls.ConnectionOptions object
  sasl: { mechanism: 'plain', username: 'u', password: 'p' },
  clientId: 'my-app',
  connectionTimeout: 1000,
  requestTimeout: 30_000,
  retry: { retries: 8 },
  logLevel: logLevel.INFO,
  logCreator: (level) => (entry) => {
    /* custom sink */
  },
});
```

SASL mechanisms: `plain`, `scram-sha-256`, `scram-sha-512`, `aws`,
`oauthbearer`, or a custom `{ mechanism, authenticationProvider }`.

See [producer configs](https://kafka.apache.org/43/configuration/producer-configs/),
[consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/),
and [SASL authentication](https://kafka.apache.org/43/security/authentication-using-sasl/).

## Extra APIs

- `AbortSignal` on `connect` / `disconnect` / `send` / `sendBatch` / `run`
- `consumer.stream()` — async iteration over batches
- `Symbol.asyncDispose` on producer, consumer, and admin (`await using`)
- Built-in ZSTD (`CompressionTypes.ZSTD`)

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

ZooKeeper vs KRaft is a broker deployment choice. The client does not embed a
ZooKeeper client. See
[KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/).
