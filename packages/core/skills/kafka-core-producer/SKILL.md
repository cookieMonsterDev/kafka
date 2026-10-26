---
name: kafka-core-producer
description: >
  Use when sending messages to Kafka with @cookiemonsterdev/kafka-core: producer.send and
  sendBatch, keys and headers, acks, compression, lingerMs batching, partitioners, delivery
  timeouts, message size limits, idempotent producing, transactions and exactly-once
  consume-transform-produce, or producer hooks.
metadata:
  purpose: >-
    Writes producer code that delivers messages with the intended durability, ordering, and
    batching behavior, and handles the producer-specific failure modes.
  type: sub-skill
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
requires:
  - kafka-core
  - kafka-core-setup
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/producer.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/producer.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/throughput.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/compatibility.md'
  - 'cookieMonsterDev/kafka:packages/core/src/producer/index.ts'
---

# kafka-core producer

This skill builds on `kafka-core`. Read it first for the rules every task shares.

## Setup

```ts
import { Kafka, CompressionTypes } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const producer = kafka.producer({ compression: CompressionTypes.GZIP });

await producer.connect();
const metadata = await producer.send({
  topic: 'events',
  messages: [{ key: 'user-1', value: JSON.stringify({ id: 1 }), headers: { 'x-trace': '1' } }],
});
console.log(metadata[0]?.baseOffset); // bigint
await producer.disconnect();
```

Connect once at startup and reuse the producer for every send. Disconnect on shutdown; it
flushes linger-buffered records.

## Core Patterns

### Choose a message key

`key` decides the partition (murmur2 by default), so records with the same key stay in order on
one partition. `value` is `string`, `Buffer`, or `null`. Serialize objects yourself
(`JSON.stringify`, Avro, Protobuf); the client does not.

### Send to several topics at once

```ts
await producer.sendBatch({
  topicMessages: [
    { topic: 'orders', messages: [{ key: 'o-1', value: 'created' }] },
    { topic: 'audit', messages: [{ value: 'order o-1 created' }] },
  ],
});
```

### Tune latency versus throughput

Defaults: `acks: -1` (all in-sync replicas), `lingerMs: 5`, `batchSize: 16384`,
`maxInFlightRequests: 5`, `idempotent: false`. Consecutive `send()` calls within `lingerMs` share
one Produce request. Use `lingerMs: 0` for one request per `send()`. For high volume, spread the
preset:

```ts
import { throughputPreset } from '@cookiemonsterdev/kafka-core';

const producer = kafka.producer({ ...throughputPreset().producer });
```

The preset enables the sticky partitioner and a 32 MiB send buffer. GZIP, Snappy, LZ4, and ZSTD
are built in (ZSTD needs Kafka 2.1+); no codec packages are needed.

### Bound delivery time

`deliveryTimeoutMs` (default `120000`) caps one `send()` end to end, including linger, buffer
waits, and retries. `timeout` on `send()` (default `30000`) is only the broker-side deadline for
one Produce request.

```ts
import { KafkaDeliveryTimeoutError, KafkaMessageTooLargeError } from '@cookiemonsterdev/kafka-core';

const producer = kafka.producer({ deliveryTimeoutMs: 30_000, retry: { retries: 3 } });

try {
  await producer.send({ topic: 'events', messages: [{ value: 'hello' }] });
} catch (error) {
  if (error instanceof KafkaDeliveryTimeoutError) {
    // Unknown outcome: the record may or may not have reached the broker.
  } else if (error instanceof KafkaMessageTooLargeError) {
    // Over maxRequestSize (default 1 MiB, uncompressed). Split the payload or raise the cap.
  }
}
```

### Exactly-once with transactions

Set `transactionalId` (stable per producer instance) and `idempotent: true`. Requires Kafka 0.11+.

```ts
const producer = kafka.producer({ transactionalId: 'payments-writer-1', idempotent: true });
await producer.connect();

const transaction = await producer.transaction();
try {
  await transaction.send({ topic: 'payments', messages: [{ key: 'p-1', value: 'captured' }] });
  await transaction.sendOffsets({
    consumerGroupId: 'payments-processor',
    topics: [{ topic: 'payment-requests', partitions: [{ partition: 0, offset: 43n }] }],
  });
  await transaction.commit();
} catch (error) {
  await transaction.abort();
  throw error;
}
```

`sendOffsets` commits the consumer's progress inside the same transaction; pass the offset of the
next record to read (last processed + `1n`). Consumers read committed data only by default
(`readUncommitted: false`).

For producer `hooks` (`onSend`, `onAck`), partitioner choices, and `AbortSignal`, read
[producer options](references/options.md).

## Common Mistakes

- **CRITICAL — Treating `KafkaDeliveryTimeoutError` as "not sent".** An in-flight attempt is not
  cancelled and can still land. Make consumers idempotent or use transactions when duplicates
  matter. Source: core guides/producer.
- **HIGH — Retrying a fenced transactional producer.** `PRODUCER_FENCED` or
  `INVALID_PRODUCER_EPOCH` means another instance took over the `transactionalId`. Every later
  call on this producer rejects; create a new producer. Source: core reference/producer.
- **HIGH — Calling `commit()` after a failed `send()` in a transaction.** That transaction can
  only be aborted. Abort, then start a new one. Source: core reference/producer.
- **HIGH — One producer (and connection set) per request.** Create the producer once; it is safe
  to share across concurrent `send()` calls.
- **MEDIUM — Expecting kafkajs's send-immediately behavior.** This client lingers 5 ms by default.
  Use `lingerMs: 0` if per-call latency matters more than batching, or `await producer.flush()`.
- **MEDIUM — Headers or transactions against Kafka 0.10.** MessageSet has no headers,
  transactions, or idempotence; the call throws a non-retriable error.

## Completion

- `send()` resolves with `RecordMetadata[]` whose `baseOffset` is a `bigint`.
- A test consumer (or `kafka topic offsets <topic>` from `@cookiemonsterdev/kafka-cli`) sees the
  new records.
- Shutdown calls `producer.disconnect()` so linger-buffered records are flushed.

## References

- [Producer options](references/options.md): read when configuring hooks, partitioners,
  `maxRequestSize`, or cancellation with `AbortSignal`.
