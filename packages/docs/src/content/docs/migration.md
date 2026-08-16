---
title: Migration from kafkajs
description: Deliberate breaking changes versus kafkajs 2.2.4
order: 4
---

The public shape is `new Kafka({…}).producer() / consumer() / admin()`. These
are the exceptions; everything else is source-compatible.

## 1. Offsets are `bigint`, not `string`

`RecordMetadata.baseOffset`, `message.offset`, `admin.fetchTopicOffsets()`,
`consumer.seek({ offset })`, commit APIs, and the rest of the offset surface
use `bigint`.

```ts
// before
console.log(message.offset); // '42'
await consumer.seek({ topic, partition, offset: '42' });

// after
console.log(message.offset); // 42n
await consumer.seek({ topic, partition, offset: 42n });
```

`seek` / `commitOffsets` / admin offset inputs still accept `number` and
`string` at runtime so existing callers do not fail on a types-only change;
prefer `bigint` in new code.

## 2. Brokers older than Kafka 3.0 are unsupported

The client talks KRaft-era protocol versions only. There is no ZooKeeper
support.

## 3. Message-set v0/v1 is gone

RecordBatch v2 is the only record format. A cluster that still produces
legacy message-sets is outside the supported range (see 2).

## 4. ZSTD is built in

`CompressionTypes.ZSTD` uses Node 24's native `zlib.zstdCompress` /
`zstdDecompress`. Snappy and LZ4 stay pluggable:

```ts
import { CompressionCodecs, CompressionTypes } from '@kafka/core';

CompressionCodecs[CompressionTypes.Snappy] = () => mySnappyCodec;
```

## 5. Generated types

Declaration files come from the TypeScript source, so type _shapes_ match the
implementation but declaration _identity_ is not the old hand-maintained
`kafkajs/types/index.d.ts`. Code that declaration-merges those names needs an
update. Some type _names_ also differ (`CustomPartitioner` instead of
`ICustomPartitioner`, `TopicConfig` instead of `ITopicConfig`).

## Environment variables kept for compatibility

| Variable                           | Effect                                  |
| ---------------------------------- | --------------------------------------- |
| `KAFKAJS_NO_PARTITIONER_WARNING=1` | Silence the default-partitioner warning |
| `KAFKAJS_LOG_LEVEL`                | Override the configured log level       |
