---
title: Migrating from kafkajs
description: Replace kafkajs with a maintained client that keeps the same producer, consumer, and admin API shape
order: 0
section: migration
sidebarLabel: From kafkajs
---

[kafkajs](https://github.com/tulios/kafkajs) has not shipped a release since 2023. `@cookiemonsterdev/kafka-core` keeps the same API shape —
`new Kafka({ clientId, brokers })`, `producer()`, `consumer()`, `admin()`,
`eachMessage` / `eachBatch`, `logLevel`, `CompressionTypes`, `Partitioners`,
`PartitionAssigners` — so most code moves over with an import change and a
pass over offsets.

## 1. Swap the package

```sh
npm uninstall kafkajs
npm install @cookiemonsterdev/kafka-core
```

```diff
- import { Kafka, logLevel, CompressionTypes } from 'kafkajs';
+ import { Kafka, logLevel, CompressionTypes } from '@cookiemonsterdev/kafka-core';
```

The package is ESM and needs **Node.js 24+** (Bun is supported too). See
[Installation](../../start/installation/).

## 2. Offsets and timestamps are `bigint`

kafkajs returns offsets and timestamps as strings. This client returns `bigint`:

```diff
  await consumer.run({
    eachMessage: async ({ message }) => {
-     const next = (BigInt(message.offset) + 1n).toString();
+     const next = message.offset + 1n;
    },
  });
```

`JSON.stringify` throws on `bigint`, so convert at the boundary
(`offset.toString()`). `consumer.seek()` still accepts `bigint | number | string`.

## 3. Rename what carried the `KafkaJS` / `KAFKAJS_` prefix

| kafkajs                                   | kafka-core                            |
| ----------------------------------------- | ------------------------------------- |
| `KafkaJSError`, `KafkaJSProtocolError`, … | `KafkaError`, `KafkaProtocolError`, … |
| `KAFKAJS_LOG_LEVEL`                       | `KAFKA_LOG_LEVEL`                     |
| `KAFKAJS_NO_PARTITIONER_WARNING`          | `KAFKA_NO_PARTITIONER_WARNING`        |
| `Partitioners.JavaCompatiblePartitioner`  | `Partitioners.DefaultPartitioner`     |

Other error classes drop the `JS` the same way (`KafkaJSNumberOfRetriesExceeded`
→ `KafkaNumberOfRetriesExceeded`). The full list is in
[Errors](../../reference/errors/).

## 4. Drop the codec packages

Snappy, LZ4, and ZSTD are built in. Remove `kafkajs-snappy`, `kafkajs-lz4`,
`@kafkajs/zstd`, and the `CompressionCodecs[...] = ...` registration lines.
`CompressionCodecs` is still exported if you want to override a codec.

## 5. Check the producer defaults

kafkajs sends each `send()` immediately. This client lingers for up to
`lingerMs: 5` and batches up to `batchSize: 16384` bytes. Pass `lingerMs: 0` to
keep one Produce request per `send()`. The other defaults are in
[Compatibility](../../reference/compatibility/#constructor-defaults).

## What you gain

- Kafka 4.x support, including the KIP-848 consumer protocol
  (`groupProtocol: 'consumer'`) and KIP-932 share groups (`kafka.shareConsumer()`)
- Built-in compression codecs with no native addons
- Generated TypeScript types, `AbortSignal` on connect/send/run, and
  `await using` via `Symbol.asyncDispose`
- A [CLI](../../../cli/start/introduction/) and a local web
  [Studio](../../../studio/start/introduction/) built on the same client

For the remaining differences, see [Breaking changes](../breaking-changes/).
