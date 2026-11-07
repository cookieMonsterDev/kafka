---
title: Offsets and manual assignment
description: Assign partitions, seek, pause, and inspect committed offsets and lag
order: 22
section: guides
---

Use [Consumer](../consumer/) for the connection and subscription basics.

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

## Committed offsets, position, and lag

```ts
const committed = await consumer.committed([{ topic: 'events', partition: 0 }]);
const position = consumer.position({ topic: 'events', partition: 0 });
const lag = consumer.currentLag({ topic: 'events', partition: 0 });
```

`committed()` fetches offsets from the group coordinator (OffsetFetch); it
does not need `run()`/`stream()` to have started, since it is reading broker
state rather than local state. A partition with no committed offset comes
back as `offset: -1n`. This is the live-consumer counterpart to
`admin.fetchOffsets`, which remains the out-of-band tool for inspecting or
resetting offsets without a running consumer.

`position()` is the next offset this consumer will read for an assigned
partition; `currentLag()` is `highWatermark - position()`. Both return `null`
when the partition is not currently assigned instead of throwing - so a
rebalance that moves a partition away shows up as `null`, not an error.
`currentLag()` also returns `null` until at least one Fetch response has been
seen for the partition. Both require the group/assignment to have started
(`run()`, `stream()`, or `assign()`) and throw otherwise.
