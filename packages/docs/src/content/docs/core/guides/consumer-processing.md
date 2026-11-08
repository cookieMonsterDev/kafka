---
title: Process messages and batches
description: Choose eachMessage, eachBatch, stream, and delivery limits
order: 21
section: guides
---

Use [Consumer](../consumer/) for the connection and subscription basics.

## eachMessage vs eachBatch vs stream

`run({ eachMessage })` is the usual path. `eachBatch` plus
`partitionsConsumedConcurrently` is the heavy-load consume API: you get the
whole fetch batch plus `resolveOffset` / `commitOffsetsIfNecessary`, and can
process partitions in parallel. The run default for concurrency remains `1`.
Spread `throughputPreset().consumer` into `run()` to set concurrency to `4`.
See [Throughput](../throughput/). `consumer.stream()` is an async iterator over
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
and [Share groups](../share-consumers/#share-groups-kip-932)).

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
