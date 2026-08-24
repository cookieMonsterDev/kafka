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

## maxRecords

Fetch size is byte-driven (`maxBytesPerPartition`, `maxBytes`): the broker can
return a batch holding far more records than an application wants to process
before checkpointing. `run({ maxRecords })` (and `stream({ maxRecords })`)
caps how many records reach the handler per internal delivery cycle of an
already-fetched batch. It never changes the Fetch request itself —
`maxBytesPerPartition` and `maxBytes` stay exactly as configured, and the
broker keeps returning full-size batches. This only slices how many of the
already-returned records are handed to the handler at once; any remainder
carries over to the next internal cycle instead of being dropped or
re-fetched. The protocol/ecosystem term for the same idea is
[max.poll.records](https://kafka.apache.org/43/configuration/consumer-configs/#max.poll.records).

The default differs by handler:

- `eachMessage` defaults to `500`. An oversized fetched batch is delivered in
  slices of at most 500 records, with an offset-commit checkpoint between
  slices so a crash partway through the batch does not lose offsets that were
  already processed.
- `eachBatch` is unlimited unless `maxRecords` is set explicitly — a batch
  handler is designed to receive a whole batch by default. Setting
  `maxRecords` splits an oversized batch into smaller sub-batches instead,
  each with its own `resolveOffset` and a checkpoint between slices.

```ts
await consumer.run({
  maxRecords: 100,
  eachMessage: async ({ message }) => {
    console.log(message.offset);
  },
});
```

This is unrelated to the share consumer's `maxRecords` (a ShareFetch wire
field capping records per ShareFetch response; see
[`ShareConsumerConfig`](../../reference/configuration/#shareconsumerconfig)
and [Share groups](#share-groups-kip-932) below).

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

## Hooks

`hooks` is a set of ordered async callbacks, not an interceptor SPI - there is
no `ConsumerInterceptor` class to implement. `onConsume` fires immediately
before the user's handler runs: once per message in `eachMessage` mode, once
per batch in `eachBatch` mode (exactly one of `message`/`batch` is set on the
event, matching the active mode). `onCommit` fires once per offset-commit
attempt - auto-commit or a manual `commitOffsets()` call - after the broker
responds, with `error` set on failure. Each array runs in registration order,
one hook is always awaited before the next starts, and a hook that throws is
caught, logged, and never affects consumption or the commit - it neither
blocks a message/batch nor turns a successful commit into a failure (or vice
versa).

```ts
const consumer = kafka.consumer({
  groupId: 'my-group',
  hooks: {
    onConsume: [({ topic, partition, message }) => console.log('consuming', topic, partition, message?.offset)],
    onCommit: [
      ({ topics, error }) => {
        if (error) console.error('commit failed', error);
        else console.log('committed', topics.length, 'topic(s)');
      },
    ],
  },
});
```

## Rebalance callbacks

`run()` accepts three optional callbacks for observing group membership
changes. Each is `(topicPartitions: { topic: string; partition: number }[]) =>
void | Promise<void>` and is called with only the partitions actually moving
in that rebalance step, not the full assignment:

```ts
await consumer.run({
  onPartitionsRevoked: async (partitions) => {
    // Commit offsets for these partitions before they're fetched by anyone else.
    console.log('revoked', partitions);
  },
  onPartitionsAssigned: async (partitions) => {
    console.log('assigned', partitions);
  },
  onPartitionsLost: async (partitions) => {
    console.log('lost', partitions);
  },
  eachMessage: async ({ topic, partition, message }) => {
    /* ... */
  },
});
```

- **`onPartitionsRevoked`** fires with the partitions this member is giving
  up, before the consumer fetches from its new assignment (revoke happens
  before reassignment). For the round-robin default and the other eager
  assigners, a rebalance revokes the member's entire prior assignment, so this
  fires with everything the member held, immediately followed by
  `onPartitionsAssigned` with the entire new assignment. For the
  cooperative-sticky assigner (KIP-429), only the subset actually being given
  up this round is reported - partitions the member keeps across the
  rebalance are never passed to this callback. See
  [Assigners and isolation](#assigners-and-isolation).
- **`onPartitionsAssigned`** fires with the partitions newly gained once the
  member has installed its new assignment. Same eager-vs-incremental split as
  above: the entire new assignment for eager assigners, only the newly gained
  subset for cooperative-sticky.
- **`onPartitionsLost`** fires **instead of** `onPartitionsRevoked` when the
  member's assignment was lost without a clean revoke - its session expired,
  or it was fenced out of the group (`UNKNOWN_MEMBER_ID` / broker rejects a
  stale generation) before it had a chance to leave gracefully. This is the
  signal to _abandon_ any pending offset commit for those partitions rather
  than attempt it: a lost partition may already be owned by another member,
  so committing against it can race the new owner's progress or simply get
  rejected. `onPartitionsRevoked`, by contrast, always fires while the member
  is still a recognized part of the group, so committing there is safe. A
  callback never receives both events for the same partitions in the same
  rebalance.
- KIP-848's `ConsumerGroupHeartbeat` protocol reconciles assignments
  incrementally at the wire level, so `onPartitionsRevoked` /
  `onPartitionsAssigned` report the same kind of incremental diff there as
  they do for cooperative-sticky, regardless of `groupProtocol`.
- Each callback is awaited before the consumer proceeds -
  `onPartitionsRevoked` (or `onPartitionsLost`) completes before the new
  assignment is installed, and `onPartitionsAssigned` completes before the
  consumer fetches from it. An error thrown by a callback is logged and does
  not abort the rebalance or the rejoin; a broken user-supplied callback
  should not be able to break group membership.
- These callbacks are additive: `events.REBALANCING` and `events.GROUP_JOIN`
  (via `consumer.on(...)`) keep firing exactly as before. `REBALANCING`
  signals "the group needs a rejoin"; `GROUP_JOIN` reports the outcome once
  membership stabilizes. The callbacks above tell you specifically which
  partitions moved and why, without parsing group state yourself. Ecosystem
  term for the same protocol concept: Java's `ConsumerRebalanceListener`.

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
