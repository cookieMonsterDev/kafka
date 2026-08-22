---
title: Producer
description: send, sendBatch, acks, linger, partitioners, and abort
order: 1
section: guides
---

```ts
import { Kafka, CompressionTypes, Partitioners } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const producer = kafka.producer({
  acks: -1,
  compression: CompressionTypes.GZIP,
});

await producer.connect();
const metadata = await producer.send({
  topic: 'events',
  messages: [{ key: 'user-1', value: 'hello', headers: { 'x-trace': '1' } }],
});
console.log(metadata[0]?.baseOffset); // bigint
await producer.disconnect();
```

Methods live on [`Producer`](../../reference/producer/). Config fields are in
[Configuration](../../reference/configuration/). Source:
[`producer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/producer/index.ts).

## Message shape

Each `Message` can set `key`, `value`, `headers`, `partition`, and `timestamp`.
`value` may be `string`, `Buffer`, or `null`. See
[Messages](https://kafka.apache.org/43/implementation/messages/).

## Acks, linger, and batching

Producer-level `acks` (default `-1`, all ISR) and `compression` apply when a
call omits them. `lingerMs` defaults to `0`, so each `send()` is its own Produce
request. Set `lingerMs` / `batchSize` to batch, or spread
[`throughputPreset()`](./throughput/). Java 4.0 default `linger.ms` is 5 ms;
the **next major** of this client will match that (see
[Breaking changes](../../migration/breaking-changes/)). See
[producer configs](https://kafka.apache.org/43/configuration/producer-configs/).

GZIP, Snappy, LZ4, and ZSTD are built in (`CompressionTypes.GZIP` / `.Snappy` /
`.LZ4` / `.ZSTD`). Kafka Snappy uses xerial snappy-java framing. LZ4 uses the
LZ4 Frame format (LZ4F) that Apache Kafka writes for magic-2 record batches.
ZSTD needs Kafka 2.1+ (Produce v7). GZIP and ZSTD use Node’s zlib threadpool.
Snappy and LZ4 run off-thread (`worker_threads`; optional native packages if
installed). Built-in codecs remain overridable via `CompressionCodecs`.
Prefer GZIP or ZSTD under load; Snappy and LZ4 are also safe for the event
loop now that they are off-thread.

## Partitioners

The default is murmur2 (`Partitioners.DefaultPartitioner`). Pass
`createPartitioner: Partitioners.LegacyPartitioner` for pre-2.0 key routing.
For KIP-794 uniform sticky routing, opt in with
`createPartitioner: Partitioners.StickyPartitioner`. Explicit partitions are
honored, keyed records continue to use Java-compatible murmur2, and unkeyed
records share a partition for each producer batch before rotating uniformly.
The default remains unchanged. See [Compatibility](../../reference/compatibility/).

## Idempotence and abort

`idempotent` defaults to `false` (Java 3.0+ defaults `enable.idempotence` to
`true`). `send()` and `sendBatch()` accept `signal` to abort the wait.

```ts
await producer.send({ topic: 'events', messages: [{ value: 'hello' }], signal });
```

Transactions: [Producer API](../../reference/producer/#transaction).
