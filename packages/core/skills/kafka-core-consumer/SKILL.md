---
name: kafka-core-consumer
description: >
  Use when reading messages from Kafka with @cookiemonsterdev/kafka-core: consumer groups,
  subscribe and run with eachMessage or eachBatch, async-iterator streaming, offset commits,
  fromBeginning and auto offset reset, seek/pause/resume, manual partition assignment, rebalance
  callbacks, lag, the KIP-848 consumer protocol, or queue-style share groups (KIP-932).
metadata:
  purpose: >-
    Writes long-running consumer code that processes each record with the intended delivery
    guarantee, commits offsets correctly, and survives rebalances and shutdown.
  type: sub-skill
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
requires:
  - kafka-core
  - kafka-core-setup
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/consumer.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/consumer.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/throughput.md'
  - 'cookieMonsterDev/kafka:packages/core/src/consumer/types.ts'
---

# kafka-core consumer

This skill builds on `kafka-core`. Read it first for the rules every task shares.

## Setup

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'my-group' });

await consumer.connect();
await consumer.subscribe({ topics: ['events'], fromBeginning: true });
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const value = message.value?.toString();
    console.log({ topic, partition, offset: message.offset, value });
  },
});

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void consumer.disconnect();
  });
}
```

Consumers are long-running processes. `key` and `value` arrive as `Buffer | null`; deserialize
them yourself. `message.offset` and `message.timestamp` are `bigint`.

## Core Patterns

### Choose where a new group starts

`fromBeginning: true` starts at the earliest offset when the group has no committed offset;
otherwise the default is latest. `autoOffsetReset: 'none'` throws instead, and
`autoOffsetReset: 'by_duration:PT1H'` starts one hour back (KIP-1106). A group that already has
commits always resumes from them.

### Process batches for throughput

```ts
await consumer.run({
  partitionsConsumedConcurrently: 4,
  eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
    for (const message of batch.messages) {
      if (!isRunning() || isStale()) break;
      await handle(message.value);
      resolveOffset(message.offset);
      await heartbeat();
    }
  },
});

async function handle(value: Buffer | null): Promise<void> {
  void value;
}
```

`resolveOffset` marks progress so auto-commit only covers processed records. `eachMessage`
delivers at most 500 records between commit checkpoints (`maxRecords`); `eachBatch` is unlimited
unless `maxRecords` is set. `...throughputPreset().consumer` sets concurrency to 4.

### Iterate with `for await`

```ts
for await (const batch of consumer.stream()) {
  for (const message of batch.messages) {
    console.log(message.offset);
  }
}
```

`stream()` and `run()` cannot both run on one consumer.

### Commit offsets yourself

With `autoCommit: false`, commit the offset of the **next** record to read:

```ts
await consumer.run({
  autoCommit: false,
  eachMessage: async ({ topic, partition, message }) => {
    await handle(message.value);
    await consumer.commitOffsets([{ topic, partition, offset: message.offset + 1n }]);
  },
});
```

### Pause, resume, seek, lag

```ts
consumer.pause([{ topic: 'events' }]);
consumer.resume([{ topic: 'events' }]);
consumer.seek({ topic: 'events', partition: 0, offset: 42n });

const committed = await consumer.committed([{ topic: 'events', partition: 0 }]); // offset -1n = none
const lag = consumer.currentLag({ topic: 'events', partition: 0 }); // bigint | null
```

`position()` and `currentLag()` return `null` for a partition not currently assigned.

### React to rebalances

```ts
await consumer.run({
  onPartitionsRevoked: async (partitions) => {
    // Still a group member: safe to commit work for these partitions here.
    console.log('revoked', partitions);
  },
  onPartitionsLost: async (partitions) => {
    // Fenced or session expired: another member may own these. Do not commit; drop pending work.
    console.log('lost', partitions);
  },
  onPartitionsAssigned: async (partitions) => {
    console.log('assigned', partitions);
  },
  eachMessage: async ({ message }) => {
    await handle(message.value);
  },
});
```

For assign mode (no group), KIP-848 (`groupProtocol: 'consumer'`), assigners, hooks, and share
groups, read [consumer modes](references/modes.md).

## Common Mistakes

- **CRITICAL — Committing `message.offset` instead of `message.offset + 1n`.** The committed
  offset is the next one to read, so committing the processed offset re-delivers that record
  after a restart.
- **CRITICAL — Committing in `onPartitionsLost`.** A lost partition may already belong to
  another member, so the commit races the new owner or is rejected. Commit in
  `onPartitionsRevoked` only. Source: core guides/consumer.
- **HIGH — Calling `run()` inside a serverless or HTTP request handler.** The consumer must stay
  connected to keep its group membership. Run it in a long-lived worker process.
- **HIGH — Doing slow work in `eachBatch` without `heartbeat()` or the `isRunning`/`isStale`
  checks.** The member misses the session timeout, gets fenced, and the batch is redelivered.
- **HIGH — Mixing `assign()` with `subscribe()`, or `run()` with `stream()`, on one consumer.**
  Both pairs throw. Create separate consumers.
- **MEDIUM — Using a JavaScript-only regex (lookaround, backreferences) with
  `groupProtocol: 'consumer'`.** The broker evaluates it as RE2. Source: core guides/consumer.
- **MEDIUM — Forgetting `disconnect()` on shutdown.** The member lingers in the group until the
  session times out, delaying the rebalance for everyone else.

## Completion

- `consumer.on(consumer.events.GROUP_JOIN, …)` (or `onPartitionsAssigned`) fires, and the
  handler receives test records.
- After a restart, it resumes after the last processed record, not before or past it
  (`consumer.committed()` shows `last processed + 1n`).
- `SIGTERM` triggers `consumer.disconnect()`.

## References

- [Consumer modes](references/modes.md): read when using assign mode, KIP-848, partition
  assigners, consumer hooks, or share groups (`kafka.shareConsumer`).
