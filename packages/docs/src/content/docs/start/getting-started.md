---
title: Getting started
description: Create a client and send or consume messages
order: 3
section: start
---

Create one `Kafka` instance per process. Offsets are `bigint` (`42n`).

```ts
import { Kafka, CompressionTypes, logLevel } from '@kafka/core';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'events',
  compression: CompressionTypes.GZIP,
  messages: [{ key: 'user-1', value: 'hello' }],
});
await producer.disconnect();

const consumer = kafka.consumer({ groupId: 'my-group' });
await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset, value: message.value?.toString() });
  },
});
```

`await using` calls `disconnect()`:

```ts
await using producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'events', messages: [{ value: 'hello' }] });
```

Assume a broker at `localhost:9092`. Cluster concepts (topics, partitions, groups)
are in the [Apache Kafka introduction](https://kafka.apache.org/43/getting-started/introduction/).

Next: [Producer](../guides/producer/), [Consumer](../guides/consumer/),
[Errors](../guides/errors/), [Compatibility](../reference/compatibility/).
