---
title: Task recipes
description: Follow practical paths for replay, failed messages, authentication, and consumer lag
order: 12
section: guides
---

These recipes connect the client, CLI, and Studio documentation. Start on a local cluster
or a dedicated profile when an operation changes stored state.

## Replay recent messages

For a new replay group, use the consumer's duration-based starting policy:

```ts
const consumer = kafka.consumer({ groupId: 'replay-last-day', autoOffsetReset: 'by_duration:P1D' });
await consumer.connect();
await consumer.subscribe({ topics: ['events'] });
```

This is a fragment using the quickstart's `kafka` instance; call `run()` to process records.
The policy applies when the group has no usable commit, not as an override of an existing commit.
For an existing group, use an explicitly planned reset. See [Consumer](../consumer/#subscribe).

## Handle a failed message

Perform the side effect before marking processing successful. On failure, throw to surface
it to the consumer lifecycle; do not catch and silently return success. Persist an event id
at the destination so a redelivery does not repeat the side effect. If routing failed records
to another topic, decide when that write and the source offset commit become durable.
See [Errors](../errors/) and [Transactions](../../reference/producer/).

## Reset a consumer group

Stop its consumers first. Preview the reset, inspect the target offsets, then execute:

```sh
kafka group reset-offsets docs-demo-reader --topic docs-demo-events --to earliest
kafka group reset-offsets docs-demo-reader --topic docs-demo-events --to earliest --execute
```

Restart the consumers and inspect `kafka group offsets docs-demo-reader`. Moving backward
reprocesses records; moving forward skips records. See [reset-offsets](../../../cli/reference/commands/group/reset-offsets/).

## Connect to an authenticated cluster

Follow [Security](../security/) to configure TLS and SASL; then use `kafka doctor --profile staging`
to verify configuration resolution before using that profile for administration. The CLI and
Studio use the same [profile configuration](../../../cli/guides/configuration/#profiles).

## Investigate consumer lag

Use `kafka group describe <group>` and `kafka group offsets <group>` to identify affected
partitions. Open the group in Studio to compare membership and lag. Correlate the change
with application processing times and rebalance events before tuning concurrency.
See [Troubleshooting](../troubleshooting/#lag-grows-continuously) and [Studio walkthrough](../../../studio/start/getting-started/).
