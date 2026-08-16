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

See the [Apache Kafka introduction](https://kafka.apache.org/43/getting-started/introduction/)
for cluster concepts (topics, partitions, consumer groups). Brokers from
**Kafka 0.10** onward work; this is not Java-client 4.x parity. ZooKeeper vs
KRaft is a broker deployment detail, not something the client configures. See
[KRaft vs ZooKeeper](https://kafka.apache.org/43/getting-started/zk2kraft/)
and [Compatibility](/docs/compatibility/).

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

`idempotent` defaults to `false` (Java 3.0+ defaults `enable.idempotence` to
`true`). `lingerMs` defaults to 0 so each `send()` is its own Produce request;
set `lingerMs` / `batchSize` to batch. Java 4.0 default `linger.ms` is 5 ms.
Producer-level `acks` (default `-1`) and `compression` apply when a call omits
them.

To keep the pre-2.0 key routing, pass `createPartitioner: Partitioners.LegacyPartitioner`.
The default is the Java-compatible murmur2 partitioner, not the 4.x
sticky-until-batch-size partitioner.

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
`run()`. Isolation defaults to `read_committed` (`readUncommitted: false`);
Java default `isolation.level` is `read_uncommitted`. `fromBeginning` is
boolean (earliest vs latest); there is no `auto.offset.reset=none`.

Range, RoundRobin, Sticky, and CooperativeSticky are built in
(`PartitionAssigners`); the default is round-robin. Groups use the classic
protocol only. Defaults that differ from the Java client are listed under
[Compatibility](/docs/compatibility/).

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
`alterConfigs` remains for older brokers; prefer `incrementalAlterConfigs`.
`electLeaders` and `deleteGroupOffsets` are implemented. See
[Compatibility](/docs/compatibility/).
