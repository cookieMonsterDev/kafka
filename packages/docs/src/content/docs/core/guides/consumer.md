---
title: Consumer
description: subscribe, run, stream, pause, seek, and assigners
order: 2
section: guides
---

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'my-group' });
// Opt in to KIP-848 on Kafka 4.0+: kafka.consumer({ groupId: 'my-group', groupProtocol: 'consumer' })

await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset, value: message.value?.toString() });
  },
});
```

`message.offset` is `bigint`. Methods: [`Consumer`](../../reference/consumer/).
Source:
[`consumer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/consumer/index.ts).

Consumers are long-running processes. Do not call `run()` inside a serverless
request handler.

`listTopics()` and `partitionsFor(topic)` are the same Metadata helpers as on
the producer. See [Consumer API](../../reference/consumer/#listtopics--partitionsfor).

## Subscribe

`fromBeginning: true` starts at the earliest offset when the group has no
commit. Pass `autoOffsetReset: 'none'` to throw instead. Pass
`autoOffsetReset: 'by_duration:PT1H'` (KIP-1106) to start at the first offset
at or after `now` minus that ISO-8601 duration, resolved via ListOffsets.
See
[auto.offset.reset](https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset).

## Choose your next step

- [Process messages and batches](../consumer-processing/) — Choose eachMessage, eachBatch, stream, and delivery limits.

## eachMessage vs eachBatch vs stream

Continue to [Process messages and batches](../consumer-processing/#eachmessage-vs-eachbatch-vs-stream).

## maxRecords

Continue to [Process messages and batches](../consumer-processing/#maxrecords).

## Hooks

Continue to [Process messages and batches](../consumer-processing/#hooks).

- [Offsets and manual assignment](../consumer-offsets/) — Assign partitions, seek, pause, and inspect committed offsets and lag.

## Pause, resume, seek

Continue to [Offsets and manual assignment](../consumer-offsets/#pause-resume-seek).

## Assign mode

Continue to [Offsets and manual assignment](../consumer-offsets/#assign-mode).

## Committed offsets, position, and lag

Continue to [Offsets and manual assignment](../consumer-offsets/#committed-offsets-position-and-lag).

- [Groups and rebalances](../consumer-rebalances/) — Choose assigners and protocols, and handle partition lifecycle callbacks.

## Assigners and isolation

Continue to [Groups and rebalances](../consumer-rebalances/#assigners-and-isolation).

## Rebalance callbacks

Continue to [Groups and rebalances](../consumer-rebalances/#rebalance-callbacks).

- [Share consumers](../share-consumers/) — Acquire and acknowledge queue-like records on Kafka 4.1+.

## Share groups (KIP-932)

Continue to [Share consumers](../share-consumers/#share-groups-kip-932).

### Server-side assignor and regex subscription (KIP-848)

Continue to [Server-side assignor and regex subscription](../consumer-rebalances/#server-side-assignor-and-regex-subscription-kip-848).

### Acknowledgement: implicit vs explicit

Continue to [Acknowledgement modes](../share-consumers/#acknowledgement-implicit-vs-explicit).

### Instrumentation events

Continue to [Share-consumer instrumentation](../share-consumers/#instrumentation-events).
