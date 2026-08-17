---
title: Consumer
description: subscribe, run, stream, pause, seek, and assigners
order: 2
section: guides
---

```ts
import { Kafka } from '@kafka/core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'my-group' });

await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset, value: message.value?.toString() });
  },
});
```

`message.offset` is `bigint`. Methods: [`Consumer`](../reference/consumer/).
Source:
[`consumer/index.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/consumer/index.ts).

Consumers are long-running processes. Do not call `run()` inside a serverless
request handler.

## Subscribe

`fromBeginning: true` starts at the earliest offset when the group has no
commit. Pass `autoOffsetReset: 'none'` to throw instead. See
[auto.offset.reset](https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset).

## eachMessage vs eachBatch vs stream

`run({ eachMessage })` is the usual path. `eachBatch` gives the whole fetch
batch plus `resolveOffset` / `commitOffsetsIfNecessary`. `consumer.stream()` is
an async iterator over batches; it cannot run alongside `run()`.

```ts
for await (const batch of consumer.stream()) {
  for (const message of batch.messages) {
    console.log(message.offset);
  }
}
```

`run({ signal })` stops when the signal aborts.

## Pause, resume, seek

```ts
consumer.pause([{ topic: 'events' }]);
consumer.resume([{ topic: 'events' }]);
consumer.seek({ topic: 'events', partition: 0, offset: 42n });
```

## Assigners and isolation

Range, round-robin (default), sticky, and cooperative-sticky are built in
(`PartitionAssigners`). Groups use the classic protocol only — there is no
KIP-848 `group.protocol=consumer`. Isolation defaults to `read_committed`
(`readUncommitted: false`); Java default `isolation.level` is
`read_uncommitted`. See [Compatibility](../reference/compatibility/) and
[consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).
