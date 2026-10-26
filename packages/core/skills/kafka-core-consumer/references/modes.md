# Consumer modes

## Assign mode (no consumer group)

`assign()` reads exact partitions with no group membership and no rebalancing. Use it for a
fixed worker-per-partition layout or for replaying one partition.

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({}); // groupId is optional here
await consumer.connect();
await consumer.assign([
  { topic: 'events', partition: 0 },
  { topic: 'events', partition: 1 },
]);
await consumer.run({
  eachMessage: async ({ message }) => {
    console.log(message.offset);
  },
});
```

Assign mode never auto-commits. `consumer.commitOffsets()` needs a `groupId` and throws without
one. Starting position: an earlier `seek()`, else the group's committed offset (if `groupId` is
set), else `autoOffsetReset` (default `latest`).

## KIP-848 consumer protocol

Classic JoinGroup/SyncGroup is the default. On Kafka 4.0+ opt in with
`groupProtocol: 'consumer'`: the broker assigns partitions, `heartbeatInterval` and
`sessionTimeout` are unused for membership, and `groupRemoteAssignor: 'uniform' | 'range'` picks
the broker-side assignor. A `RegExp` subscription is matched by the broker as RE2 (Kafka 4.1+),
one pattern per group.

```ts
const consumer = kafka.consumer({ groupId: 'my-group', groupProtocol: 'consumer', groupRemoteAssignor: 'range' });
```

Inspect KIP-848 groups with `admin.describeConsumerGroups`, classic groups with
`admin.describeClassicGroups`.

## Assigners (classic protocol)

`PartitionAssigners`: round-robin (default), range, sticky, and cooperative-sticky. Eager
assigners revoke the whole assignment on every rebalance; cooperative-sticky (KIP-429) revokes
only partitions that move.

## Isolation

`readUncommitted` defaults to `false` (`read_committed`): records from aborted or open
transactions are not delivered.

## Hooks

`hooks.onConsume` fires before the handler (per message or per batch); `hooks.onCommit` fires
after each commit attempt with `error` on failure. A hook that throws is logged and never affects
consumption.

## Share groups (KIP-932, Kafka 4.1+)

`kafka.shareConsumer({ groupId })` gives queue-style delivery: any member may receive any record,
several members can drain one partition, and records held by a crashed member are redelivered.
There is no partition assignment and no `seek`. Use `consumer()` instead when partition ordering,
`seek`/`pause`, or offset control matters.

```ts
import { Kafka, SHARE_ACKNOWLEDGE_TYPE } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const share = kafka.shareConsumer({ groupId: 'share-events' });
await share.connect();
share.subscribe({ topics: ['events'] }); // synchronous

await share.run({
  eachBatchAutoAck: false,
  eachBatch: async ({ batch, acknowledge }) => {
    const ok = batch.messages.every((message) => message.value !== null);
    acknowledge(ok ? SHARE_ACKNOWLEDGE_TYPE.ACCEPT : SHARE_ACKNOWLEDGE_TYPE.REJECT);
  },
});
```

`eachMessage` (and `eachBatch` with the default `eachBatchAutoAck: true`) acknowledges `ACCEPT`
when the handler resolves. A handler that throws always `RELEASE`s the records for redelivery.
With `eachBatchAutoAck: false`, an unacknowledged batch stays locked until the broker's
`group.share.record.lock.duration.ms` expires. `RENEW` needs Kafka 4.2+. The broker must have
share groups enabled.
