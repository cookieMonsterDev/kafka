---
title: Breaking changes
description: Deliberate breaking changes in @kafka/core
order: 5
---

The public shape is `new Kafka({…}).producer() / consumer() / admin()`. These
are the exceptions.

## 1. Offsets are `bigint`, not `string`

`RecordMetadata.baseOffset`, `message.offset`, `admin.fetchTopicOffsets()`,
`consumer.seek({ offset })`, commit APIs, and the rest of the offset surface
use `bigint`. That includes MessageSet records on Kafka 0.10 — the decoder
converts wire offsets at the boundary, so callers never see string offsets.

```ts
console.log(message.offset); // 42n
await consumer.seek({ topic, partition, offset: 42n });
```

`seek` / `commitOffsets` / admin offset inputs still accept `number` and
`string` at runtime; prefer `bigint` in new code.

## 2. Brokers from Kafka 0.10 onward

The client matches KafkaJS's support floor: **Kafka 0.10+**. Protocol versions
are negotiated from `ApiVersions`; the client does not parse a broker version
string in production.

| Range | What works                                                         |
| ----- | ------------------------------------------------------------------ |
| 0.10  | Produce/fetch via MessageSet. No headers, no transactions, no ACLs |
| 0.11+ | RecordBatch, headers, exactly-once / idempotent producers, ACLs    |
| 2.1+  | `CompressionTypes.ZSTD`                                            |
| 4.0   | KRaft-only brokers. Talks via overlap (Produce 3–7, Fetch 4–11)    |

ZooKeeper is how some test clusters and older brokers store metadata. It is
not a client feature. See
[KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/).

A broker that does not advertise a used API throws
`KafkaServerDoesNotSupportApiKey`, not an invariant error.

## 3. MessageSet and RecordBatch

Kafka 0.10 produce/fetch uses MessageSet (magic 0/1). Kafka 0.11+ uses
RecordBatch (magic 2). Fetch v4+ probes the magic byte so a cluster upgrading
from 0.10 to 0.11 can return mixed formats; the decoder stops at the first
unsupported magic in a MessageSet and finishes the rest on the next fetch.

See [Messages](https://kafka.apache.org/43/implementation/messages/).

## 4. ZSTD is built in

`CompressionTypes.ZSTD` uses Node 24's native `zlib.zstdCompress` /
`zstdDecompress`. The producer rejects ZSTD when the broker negotiated
Produce < 7 (Kafka < 2.1). Snappy and LZ4 are pluggable stubs, not built in:

```ts
import { CompressionCodecs, CompressionTypes } from '@kafka/core';

CompressionCodecs[CompressionTypes.Snappy] = () => mySnappyCodec;
```

## 5. Generated types

Declaration files come from the TypeScript source, so type shapes match the
implementation. Some type names use the unprefixed form (`CustomPartitioner`,
`TopicConfig`) rather than an `I` prefix.

## Java client defaults

This library does not match Java 4.3 producer/consumer defaults
(`enable.idempotence`, `isolation.level`, `linger.ms`, partitioner). See
[Compatibility](/docs/compatibility/).

## Environment variables

| Variable                         | Effect                                  |
| -------------------------------- | --------------------------------------- |
| `KAFKA_NO_PARTITIONER_WARNING=1` | Silence the default-partitioner warning |
| `KAFKA_LOG_LEVEL`                | Override the configured log level       |
