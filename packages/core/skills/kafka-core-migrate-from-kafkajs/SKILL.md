---
name: kafka-core-migrate-from-kafkajs
description: >
  Use when replacing kafkajs with @cookiemonsterdev/kafka-core in an existing Node.js or Bun
  codebase: swapping imports, converting string offsets to bigint, renaming KafkaJS-prefixed
  errors and KAFKAJS_ environment variables, removing kafkajs codec packages, and checking
  producer defaults that differ from kafkajs.
metadata:
  purpose: >-
    Migrates a working kafkajs codebase to kafka-core without behavior regressions, as a
    mechanical checklist followed by a verification pass.
  type: lifecycle
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
requires:
  - kafka-core
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/migration/from-kafkajs.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/migration/breaking-changes.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/compatibility.md'
---

# Migrate from kafkajs

This skill builds on `kafka-core`. The API shape is the same as kafkajs (`new Kafka({ clientId,
brokers })`, `producer()`, `consumer()`, `admin()`, `eachMessage`/`eachBatch`, `logLevel`,
`CompressionTypes`, `Partitioners`, `PartitionAssigners`), so most code moves with an import
change and a pass over offsets.

## Checks

Work through each check across the whole codebase (`rg kafkajs`, `rg KafkaJS`, `rg KAFKAJS_`,
`rg offset`), not only the first file that matches.

### 1. Runtime and package

- Inspect: `engines`, `.nvmrc`, CI Node version, Dockerfiles.
- Expected: Node.js 24+ (or Bun 1.4+). The package is ESM; a CommonJS project on Node 24 can
  still `require()` it.
- Fix: upgrade the runtime before changing code.

```sh
npm uninstall kafkajs
npm install @cookiemonsterdev/kafka-core
```

### 2. Imports

```diff
- import { Kafka, logLevel, CompressionTypes } from 'kafkajs';
+ import { Kafka, logLevel, CompressionTypes } from '@cookiemonsterdev/kafka-core';
```

Also replace `import type { … } from 'kafkajs'` and any `require('kafkajs')`.

### 3. Offsets and timestamps are `bigint`

- Inspect: every use of `message.offset`, `message.timestamp`, `batch.highWatermark`,
  `fetchOffsets`, `fetchTopicOffsets`, `commitOffsets`, `seek`, and anything that stores or logs
  them.
- Expected: arithmetic uses `bigint` (`message.offset + 1n`); string conversions happen only at
  boundaries (`offset.toString()` before JSON, a database, or a log line).
- Failure: `BigInt(message.offset)` wrappers left in place, `parseInt(offset)`, `Number(offset)`,
  or `JSON.stringify` of an object holding an offset (throws `TypeError`).

```diff
  eachMessage: async ({ message }) => {
-   const next = (BigInt(message.offset) + 1n).toString();
+   const next = message.offset + 1n;
  },
```

`consumer.seek()` still accepts `bigint | number | string`.

### 4. Renamed errors, partitioner, and environment variables

| kafkajs                                   | kafka-core                            |
| ----------------------------------------- | ------------------------------------- |
| `KafkaJSError`, `KafkaJSProtocolError`, … | `KafkaError`, `KafkaProtocolError`, … |
| `KafkaJSNumberOfRetriesExceeded`          | `KafkaNumberOfRetriesExceeded`        |
| `Partitioners.JavaCompatiblePartitioner`  | `Partitioners.DefaultPartitioner`     |
| `KAFKAJS_LOG_LEVEL`                       | `KAFKA_LOG_LEVEL`                     |
| `KAFKAJS_NO_PARTITIONER_WARNING`          | `KAFKA_NO_PARTITIONER_WARNING`        |

Every other error class drops the `JS` the same way. Update `instanceof` checks, `error.name`
string comparisons, deployment manifests, and `.env` files.

### 5. Codec packages

Snappy, LZ4, and ZSTD are built in. Remove `kafkajs-snappy`, `kafkajs-lz4`, `@kafkajs/zstd`, and
every `CompressionCodecs[CompressionTypes.X] = …` registration.

### 6. Producer defaults

kafkajs sends each `send()` immediately. kafka-core defaults to `lingerMs: 5`,
`batchSize: 16384`, and `maxInFlightRequests: 5`. Keep these for throughput, or pass
`lingerMs: 0` where code or tests depend on one Produce request per `send()`. Consumers default
to `read_committed` isolation.

## Common Mistakes

- **HIGH — Leaving `BigInt(...)`/`.toString()` offset shims.** They still compile in places
  (`BigInt` accepts a `bigint`) but hide real string-vs-bigint bugs elsewhere. Remove them and let
  the type checker find the rest.
- **HIGH — Serializing messages or offsets to JSON without conversion.** `JSON.stringify` throws
  on `bigint`. Convert at the boundary.
- **MEDIUM — Tests that assert one broker request per `send()`.** The 5 ms linger batches them.
  Set `lingerMs: 0` in that test's producer.

## Completion

- `rg -n "kafkajs|KafkaJS|KAFKAJS_"` returns nothing outside lockfiles and changelogs.
- The project type-checks (`tsc --noEmit`) and its tests pass.
- A smoke run produces and consumes one message against a real broker and resumes from the right
  offset after a restart.

## References

- Migration guide: https://cookiemonsterdev.github.io/kafka/docs/core/migration/from-kafkajs/
- Remaining differences: https://cookiemonsterdev.github.io/kafka/docs/core/migration/breaking-changes/
