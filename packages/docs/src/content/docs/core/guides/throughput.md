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

`Partitioners.StickyPartitioner` (what the preset uses) tracks each broker
node's Produce latency and biases unkeyed-record rotation toward whichever
candidate partition's leader has responded fastest, falling back to KIP-794's
plain uniform choice once a node hasn't been measured yet. This is on by
default (`adaptive: true`); pass `StickyPartitioner({ adaptive: false })` as
`createPartitioner` for the plain uniform behavior. Keyed murmur2 routing is
unaffected either way. See [Producer](./producer/#partitioners).

### Compression level

`compressionLevel` (on `ProducerConfig`, or per `send()`/`sendBatch()` call) is
passed to whichever codec `compression` selects, but only GZIP and ZSTD have a
level to tune:

- **GZIP** maps it straight to zlib's `level` (`0`-`9`; `9` is smallest output,
  slowest to produce).
- **ZSTD** maps it to `zlib.constants.ZSTD_c_compressionLevel` (roughly
  `1`-`22`; Node's zstd binding accepts the same range as the reference
  library).
- **Snappy** and **LZ4** have no compression-level concept in either format —
  Snappy's block format and this client's LZ4 codec (`lz4-lite`, LZ4 Frame)
  are both fixed-effort. `compressionLevel` is a no-op for either.

```ts
const p = kafka.producer({
  compression: CompressionTypes.GZIP,
  compressionLevel: 6, // trade some CPU for a smaller wire payload
});

// Override per call, e.g. for a batch worth spending more CPU to shrink:
await p.send({
  topic: 'events',
  messages: [{ value: largePayload }],
  compression: CompressionTypes.ZSTD,
  compressionLevel: 19,
});
```

Higher levels cost CPU on every produced batch; benchmark against your actual
payloads before raising it broadly. A per-`send()` override is usually a
better fit than a high default when only some traffic is worth the extra CPU.

## Consume

`eachBatch` plus `partitionsConsumedConcurrently` is the heavy-load consume API.
`eachMessage` is the usual per-record path; under load it pays more per-record
await cost. The run default for `partitionsConsumedConcurrently` remains `1`
(ordering). The preset raises it to `4`.

### Fetch sessions

On Kafka **2.3+** the classic consumer keeps a per-broker **fetch session**
(KIP-227). The first Fetch to a node lists every assigned partition; later
Fetches send only partitions that were added, removed, or whose fetch offset
changed, plus a `sessionId` / `sessionEpoch`. Idle partitions stay in the
broker session without being relisted, which cuts Fetch request size when a
consumer is assigned many partitions.

Kafka **0.10–2.2** brokers do not grant sessions; those Fetches keep
`sessionId = 0` and send the full partition list every time. The share
consumer still sends its full topic list each round (it does use
`forgottenTopics` and resets the session epoch on session errors).

There is no client config for this — it is automatic when the broker
advertises Fetch sessions.

The **next major** will change constructor defaults to `lingerMs: 5`,
`batchSize: 16384`, and `maxInFlightRequests: 5`. This minor keeps
`lingerMs: 0`. See [Breaking changes](../../migration/breaking-changes/).

### checkCrcs

`ConsumerConfig.checkCrcs` defaults to `true`: every fetched record batch's
CRC (RecordBatch v2 CRC-32C, or the legacy MessageSet CRC-32 on older
brokers) is verified against the decoded bytes, and a mismatch throws
`KafkaCorruptRecordError`.

```ts
const c = kafka.consumer({ groupId: 'load', checkCrcs: false });
```

Setting `checkCrcs: false` skips that check entirely. It saves a checksum
pass over every batch, which can matter at extreme throughput, but it means
**corrupted bytes on the wire go undetected**: a bad disk on the broker, a
buggy transparent proxy, or a transport bit-flip that CRC would have caught
is instead decoded as if it were valid data — wrong record contents,
possibly without any error at all until something downstream notices. Only
disable it once you have independently verified data integrity elsewhere
(e.g. TLS already protects against in-transit corruption, and you trust the
broker's storage layer), and prefer leaving it on unless a profile shows the
check is actually the bottleneck.
