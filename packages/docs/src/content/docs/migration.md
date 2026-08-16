---
title: Breaking changes
description: Deliberate breaking changes in @kafka/core
order: 4
---

The public shape is `new Kafka({…}).producer() / consumer() / admin()`. These
are the exceptions.

## 1. Offsets are `bigint`, not `string`

`RecordMetadata.baseOffset`, `message.offset`, `admin.fetchTopicOffsets()`,
`consumer.seek({ offset })`, commit APIs, and the rest of the offset surface
use `bigint`.

```ts
console.log(message.offset); // 42n
await consumer.seek({ topic, partition, offset: 42n });
```

`seek` / `commitOffsets` / admin offset inputs still accept `number` and
`string` at runtime; prefer `bigint` in new code.

## 2. Brokers older than Kafka 3.0 are unsupported

The client talks KRaft-era protocol versions only. There is no ZooKeeper
support. See [KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/).

## 3. Message-set v0/v1 is gone

RecordBatch v2 is the only record format. A cluster that still produces
legacy message-sets is outside the supported range (see 2).
See [Messages](https://kafka.apache.org/43/implementation/messages/).

## 4. ZSTD is built in

`CompressionTypes.ZSTD` uses Node 24's native `zlib.zstdCompress` /
`zstdDecompress`. Snappy and LZ4 stay pluggable:

```ts
import { CompressionCodecs, CompressionTypes } from '@kafka/core';

CompressionCodecs[CompressionTypes.Snappy] = () => mySnappyCodec;
```

## 5. Generated types

Declaration files come from the TypeScript source, so type shapes match the
implementation. Some type names use the unprefixed form (`CustomPartitioner`,
`TopicConfig`) rather than an `I` prefix.

## Environment variables

| Variable                         | Effect                                  |
| -------------------------------- | --------------------------------------- |
| `KAFKA_NO_PARTITIONER_WARNING=1` | Silence the default-partitioner warning |
| `KAFKA_LOG_LEVEL`                | Override the configured log level       |
