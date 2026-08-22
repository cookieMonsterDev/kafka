---
title: Throughput
description: Throughput preset, linger and batching, and the heavy-load consume API
order: 7
section: guides
---

Constructor defaults stay latency-oriented in this minor (`lingerMs: 0`, murmur2,
`partitionsConsumedConcurrently: 1`). For 10k–100k msgs/s, spread
`throughputPreset()` into the producer and into `consumer.run()`, use `eachBatch`,
and set linger if you are not using the preset.

```ts
import { Kafka, throughputPreset, CompressionTypes } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'load', brokers: ['localhost:9092'] });
const { producer, consumer } = throughputPreset();

const p = kafka.producer({
  ...producer,
  compression: CompressionTypes.GZIP,
});

const c = kafka.consumer({ groupId: 'load' });
await c.connect();
await c.subscribe({ topics: ['events'] });
await c.run({
  ...consumer,
  eachBatch: async ({ batch }) => {
    for (const message of batch.messages) {
      // process
      void message;
    }
  },
});
```

`throughputPreset()` does not change constructor defaults. It returns:

| Fragment   | Fields                                                                                          |
| ---------- | ----------------------------------------------------------------------------------------------- |
| `producer` | `lingerMs: 5`, `batchSize: 16384`, `maxInFlightRequests: 5`, sticky partitioner, `bufferMemory` |
| `consumer` | `partitionsConsumedConcurrently: 4` — spread into `consumer.run()`, not `kafka.consumer()`      |

See [Compatibility](../../reference/compatibility/) and
[Configuration](../../reference/configuration/#throughputpreset).

## Produce

`lingerMs: 0` (the constructor default) sends one Produce per `send()`. Set
`lingerMs` (and usually `batchSize`) if you are not spreading the preset.
`flush()` still drains linger-buffered records.

Prefer **GZIP** or **ZSTD** for compression: both use Node’s zlib threadpool.
Snappy and LZ4 keep Kafka-compatible framing (xerial / LZ4F) and run the JS
codecs on `worker_threads`, so they no longer stall the event loop. An optional
native `snappy` or `lz4` package is used when it is installed and exposes an
async API; it is not a hard dependency.

## Consume

`eachBatch` plus `partitionsConsumedConcurrently` is the heavy-load consume API.
`eachMessage` is the usual per-record path; under load it pays more per-record
await cost. The run default for `partitionsConsumedConcurrently` remains `1`
(ordering). The preset raises it to `4`.

The **next major** will change constructor defaults to Java 4.x-aligned
`lingerMs: 5`, `batchSize: 16384`, and `maxInFlightRequests: 5`. This minor
keeps `lingerMs: 0`. See [Breaking changes](../../migration/breaking-changes/).
