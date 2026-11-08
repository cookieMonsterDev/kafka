---
title: Groups and rebalances
description: Choose assigners and protocols, and handle partition lifecycle callbacks
order: 23
section: guides
---

Use [Consumer](../consumer/) for the connection and subscription basics.

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

### Server-side assignor and regex subscription (KIP-848)

Under `groupProtocol: 'consumer'`, `groupRemoteAssignor: 'uniform' | 'range'`
requests a specific broker-side assignor (`group.remote.assignor`) instead of
letting the broker pick its default:

```ts
const consumer = kafka.consumer({
  groupId: 'my-group',
  groupProtocol: 'consumer',
  groupRemoteAssignor: 'range',
});
```

Subscribing with a `RegExp` also behaves differently under this protocol.
With the classic protocol, the client fetches topic metadata and matches the
pattern locally, then subscribes to the resolved topic names. Under
`groupProtocol: 'consumer'`, the pattern is sent as-is on
`ConsumerGroupHeartbeat` (`subscribedTopicRegex`, KIP-848
`SubscriptionPattern`) and the broker matches it against topic names itself -
no client-side metadata scan. The broker evaluates this pattern as RE2, which
is not the same syntax as JavaScript's `RegExp`; stick to RE2-compatible
patterns (the two overlap for ordinary literal and character-class matching,
but diverge on lookaround, backreferences, and some escapes). Only one
`RegExp` subscription is supported per consumer group under this protocol
(KIP-848 carries a single pattern per member) - subscribing with a second,
different pattern throws. Server-side regex requires ConsumerGroupHeartbeat
v1 (Kafka 4.1+).

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
