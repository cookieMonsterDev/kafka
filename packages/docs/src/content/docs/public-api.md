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
| `KafkaJS*` error classes                                                              | Same names as the kafkajs public errors                                |

`isRebalancing` and `isKafkaJSError` are not part of the public barrel.

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

## Additive APIs

These are extra relative to kafkajs and do not change existing call sites:

- `AbortSignal` on `connect` / `disconnect` / `send` / `sendBatch` / `run`
- `consumer.stream()` — async iteration over batches
- `Symbol.asyncDispose` on producer, consumer, and admin (`await using`)
- Built-in ZSTD (`CompressionTypes.ZSTD`)
