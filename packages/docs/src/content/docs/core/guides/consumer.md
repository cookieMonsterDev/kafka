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

## Subscribe

`fromBeginning: true` starts at the earliest offset when the group has no
commit. Pass `autoOffsetReset: 'none'` to throw instead. See
[auto.offset.reset](https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset).

## eachMessage vs eachBatch vs stream

`run({ eachMessage })` is the usual path. `eachBatch` plus
`partitionsConsumedConcurrently` is the heavy-load consume API: you get the
whole fetch batch plus `resolveOffset` / `commitOffsetsIfNecessary`, and can
process partitions in parallel. The run default for concurrency remains `1`.
Spread `throughputPreset().consumer` into `run()` to set concurrency to `4`.
See [Throughput](./throughput/). `consumer.stream()` is an async iterator over
batches; it cannot run alongside `run()`.

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

## Assign mode

`assign()` fetches exact partitions directly, with no group membership: no
JoinGroup/SyncGroup, no `ConsumerGroupHeartbeat`, no rebalancing. Use it when
you already know which partitions to read (a fixed worker-per-partition
layout, replaying a specific partition for debugging, or coordinating
assignment yourself outside Kafka's consumer groups). Use `subscribe()`
instead whenever you want the broker to divide topics across a running set of
consumers.

```ts
const consumer = kafka.consumer({}); // groupId is optional in assign mode
await consumer.connect();
await consumer.assign([
  { topic: 'events', partition: 0 },
  { topic: 'events', partition: 1 },
]);
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset });
  },
});
```

`assign()` and `subscribe()` are mutually exclusive on one consumer instance;
calling one after the other throws, and so does `run()`/`stream()` if neither
was called first. Pause, resume, and seek work exactly as they do with
`subscribe()`.

**Offset policy.** Assign mode never auto-commits, regardless of the `run()`
`autoCommit` options - there is no consumer group to own the offsets, so the
only way an offset is committed is an explicit `consumer.commitOffsets()`
call. `groupId` is optional on `kafka.consumer(...)`; it is required only if
you call `commitOffsets()` - calling it without one throws immediately rather
than committing nowhere. When a `groupId` is configured, commits are sent as
a standalone/simple consumer (no generation or membership check), so any
consumer using that group id can read them back with `OffsetFetch`, whether
or not it ever joined the group.

The starting position for each assigned partition, decided the first time
it's needed:

1. An earlier `seek()` call always wins.
2. Otherwise, if `groupId` is configured, that group's committed offset for
   the partition, when one exists (`OffsetFetch`).
3. Otherwise, `autoOffsetReset` (default `latest`), resolved with
   `ListOffsets` - the same fallback `subscribe()` uses when a group has no
   committed offset yet.

`seek()` overrides the position at any point, before or after `run()` starts.

## Assigners and isolation

Range, round-robin (default), sticky, and cooperative-sticky are built in
(`PartitionAssigners`). Cooperative-sticky follows KIP-429: members retain
unchanged partitions, revoke only partitions that move, and automatically run
the follow-up generation needed to assign revoked partitions safely. The
round-robin default and the other eager assigners still revoke their full
assignment during a rebalance.

Classic JoinGroup/SyncGroup remains the default. Set
`groupProtocol: 'consumer'` (broker property `group.protocol`) to opt into
the KIP-848 consumer protocol on Kafka 4.0+: membership and incremental
assignment use `ConsumerGroupHeartbeat` instead of JoinGroup/SyncGroup. The
broker assigns partitions (no client assignor). `heartbeatInterval` and
`sessionTimeout` are unused for membership; `rebalanceTimeout` is still sent
as the revoke budget. Use `admin.describeConsumerGroups` for KIP-848 groups
and `admin.describeClassicGroups` (alias of `describeGroups`) for classic
JoinGroup groups. Isolation defaults to `read_committed`
(`readUncommitted: false`). See [Compatibility](../../reference/compatibility/),
[KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol),
[KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol),
and [consumer configs](https://kafka.apache.org/43/configuration/consumer-configs/).

## Share groups (KIP-932)

`kafka.shareConsumer({ groupId })` is a separate API from `consumer()`. Records
are acquired, processed, then acknowledged (`ACCEPT` on success, `RELEASE` if
the handler throws). Classic subscribe remains the default path.

```ts
const share = kafka.shareConsumer({ groupId: 'share-events' });
await share.connect();
share.subscribe({ topics: ['events'] });
await share.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log({ topic, partition, offset: message.offset });
  },
});
```

Requires Kafka 4.1+ (stable ShareGroupHeartbeat v1) with share groups enabled
on the broker. Kafka 4.2+ negotiates ShareFetch / ShareAcknowledge v2
(`shareAcquireMode` for KIP-1206; `RENEW` acknowledgements for KIP-1222).
Admin helpers: `describeShareGroups`, `listShareGroupOffsets`,
`alterShareGroupOffsets`, `deleteShareGroupOffsets`, `deleteShareGroups`.
See [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka).
