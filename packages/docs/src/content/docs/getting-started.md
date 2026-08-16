---
title: Getting started
description: Create a client and send or consume messages
order: 2
---

## Install

This package is the workspace library. From the repo root:

```sh
pnpm install
pnpm --filter @kafka/core build
```

## Create a client

```ts
import { Kafka, logLevel, CompressionTypes, Partitioners } from '@kafka/core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});
```

`brokers` can also be an async function that returns the list, useful when the
set of bootstrap servers is resolved at connect time.

## Producer

```ts
const producer = kafka.producer();

await producer.connect();
await producer.send({
  topic: 'events',
  compression: CompressionTypes.GZIP,
  messages: [{ key: 'user-1', value: 'hello' }],
});
await producer.disconnect();
```

`send()` and `sendBatch()` accept an optional `signal` to abort the caller's
wait. Offsets in `RecordMetadata` are `bigint`.

To keep the pre-2.0 key routing, pass `createPartitioner: Partitioners.LegacyPartitioner`.
The default is the Java-compatible murmur2 partitioner.

`await using` works because producer, consumer, and admin implement
`Symbol.asyncDispose` (it calls `disconnect()`):

```ts
await using producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'events', messages: [{ value: 'hello' }] });
```

## Consumer

```ts
const consumer = kafka.consumer({ groupId: 'my-group' });

await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({
      topic,
      partition,
      offset: message.offset, // bigint
      value: message.value?.toString(),
    });
  },
});
```

`run({ signal })` stops the consumer when the signal aborts. `consumer.stream()`
is an async iterator over batches with backpressure; it cannot run alongside
`run()`.

## Admin

```ts
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({
  topics: [{ topic: 'events', numPartitions: 3, replicationFactor: 1 }],
});
await admin.disconnect();
```

Offset fields from `fetchTopicOffsets`, `fetchOffsets`, and friends are `bigint`.
