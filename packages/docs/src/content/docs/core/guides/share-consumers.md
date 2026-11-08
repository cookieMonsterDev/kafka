---
title: Share consumers
description: Acquire and acknowledge queue-like records on Kafka 4.1+
order: 24
section: guides
---

Use [Consumer](../consumer/) for the connection and subscription basics.

## Share groups (KIP-932)

`kafka.shareConsumer({ groupId })` is a separate API from `consumer()`. Records
are acquired, processed, then acknowledged (`ACCEPT` on success, `RELEASE` if
the handler throws). Classic subscribe remains the default path.

Reach for `consumer()` when partition ownership matters: you want a fixed
partition assignment per member, `seek`/`pause`/`resume`, offset commits you
control, or `eachBatch` semantics over a whole partition's worth of records.
Reach for `shareConsumer()` (Kafka 4.1+) when you want **queue-like**
delivery instead: any member in the group can be handed any record, several
members can cooperatively drain the same partition, and a crashed member's
in-flight records become available for another member to retry automatically.
There is no partition assignment to reason about and no `seek`.

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

### Acknowledgement: implicit vs explicit

KIP-932 calls the two acknowledgement styles `implicit` and `explicit`. Both
exist on this client's `run()`, chosen per call rather than as a single
broker-wide setting:

- **Implicit** (the default either way): after `eachMessage` resolves without
  throwing, every message it saw is acknowledged `ACCEPT` automatically.
  `eachBatch` behaves the same way as long as `eachBatchAutoAck` stays at its
  default `true` and the handler does not call `acknowledge()` itself - once
  the handler resolves, the whole batch is acknowledged `ACCEPT` for you.
- **Explicit**: inside `eachBatch`, call `acknowledge(type?)` yourself to
  control the outcome per batch - `ACCEPT` (default), `RELEASE` (make it
  available for redelivery), `REJECT`, or `RENEW` (Kafka 4.2+, KIP-1222).
  Set `eachBatchAutoAck: false` to make this mandatory: nothing is
  acknowledged unless the handler calls `acknowledge()`, so an untouched
  batch stays acquired until the broker's own lock duration
  (`group.share.record.lock.duration.ms`) expires and releases it.

A handler that throws always queues `RELEASE` regardless of ack mode, so
another member can retry the acquired range.

```ts
await share.run({
  eachBatchAutoAck: false,
  eachBatch: async ({ batch, acknowledge }) => {
    const ok = await process(batch.messages);
    acknowledge(ok ? SHARE_ACKNOWLEDGE_TYPE.ACCEPT : SHARE_ACKNOWLEDGE_TYPE.REJECT);
  },
});
```

### Instrumentation events

`share.on(eventName, listener)` mirrors `consumer.on()`: network requests
(`share.events.REQUEST` / `REQUEST_TIMEOUT` / `REQUEST_QUEUE_SIZE`, the
same shape as the classic consumer's), plus two share-consumer-specific
events.

| Event                      | Fires                                                                                                                                       |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `share.events.FETCH_START` | Before a ShareFetch round starts for a node                                                                                                 |
| `share.events.FETCH`       | After that round completes, with `numberOfBatches` and `duration`                                                                           |
| `share.events.ACKNOWLEDGE` | When acquired ranges are acknowledged - piggybacked on the next ShareFetch, or via the explicit ShareAcknowledge sent when a session closes |

```ts
share.on(share.events.ACKNOWLEDGE, ({ payload }) => {
  for (const { topic, partitions } of payload.topics) {
    for (const { partition, firstOffset, lastOffset, acknowledgeType } of partitions) {
      console.log({ topic, partition, firstOffset, lastOffset, acknowledgeType });
    }
  }
});
```
