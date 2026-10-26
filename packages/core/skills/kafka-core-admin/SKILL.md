---
name: kafka-core-admin
description: >
  Use when managing a Kafka cluster from code with @cookiemonsterdev/kafka-core's Admin client:
  creating or deleting topics, adding partitions, reading or changing topic and broker configs,
  inspecting or resetting consumer group offsets, deleting groups, ACLs, SCRAM users, delegation
  tokens, transactions, quotas, or KRaft quorum and feature operations. For one-off operations
  from a terminal, prefer the kafka-cli skill.
metadata:
  purpose: >-
    Writes Admin client code (provisioning scripts, migrations, ops tooling, test setup) that
    changes cluster state safely and checks broker support first.
  type: sub-skill
  library: '@cookiemonsterdev/kafka-core'
  library_version: '2.5.1'
requires:
  - kafka-core
  - kafka-core-setup
sources:
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/guides/admin.md'
  - 'cookieMonsterDev/kafka:packages/docs/src/content/docs/core/reference/admin.md'
  - 'cookieMonsterDev/kafka:packages/core/src/admin/types.ts'
---

# kafka-core admin

This skill builds on `kafka-core`. Read it first for the rules every task shares.

## Setup

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'provisioner', brokers: ['localhost:9092'] });

await using admin = kafka.admin();
await admin.connect();

const existing = new Set(await admin.listTopics());
if (!existing.has('events')) {
  await admin.createTopics({
    topics: [{ topic: 'events', numPartitions: 3, replicationFactor: 1 }],
    waitForLeaders: true,
  });
}
```

Use `replicationFactor: 1` only for a single-broker dev cluster; production clusters usually need 3.

## Core Patterns

### Change topic configs

Prefer `incrementalAlterConfigs` (Kafka 2.3+); it changes only the listed keys.

```ts
import { ConfigOperations, ConfigResourceTypes } from '@cookiemonsterdev/kafka-core';

await admin.incrementalAlterConfigs({
  resources: [
    {
      type: ConfigResourceTypes.TOPIC,
      name: 'events',
      configs: [{ name: 'retention.ms', value: '604800000', operation: ConfigOperations.SET }],
    },
  ],
});

const { resources } = await admin.describeConfigs({
  includeSynonyms: false,
  resources: [{ type: ConfigResourceTypes.TOPIC, name: 'events', configNames: ['retention.ms'] }],
});
```

### Inspect and move group offsets

```ts
const offsets = await admin.fetchOffsets({ groupId: 'my-group', topics: ['events'] });
// [{ topic, partitions: [{ partition, offset /* bigint */ }] }]

await admin.resetOffsets({ groupId: 'my-group', topic: 'events', earliest: true });
await admin.setOffsets({ groupId: 'my-group', topic: 'events', partitions: [{ partition: 0, offset: 100n }] });
```

Stop every consumer in the group before resetting or setting its offsets; a running member
overwrites them with its next commit.

### Topic offsets and partitions

```ts
const watermarks = await admin.fetchTopicOffsets('events'); // offset, high, low as bigint
await admin.createPartitions({ topicPartitions: [{ topic: 'events', count: 6 }] });
```

`count` is the new total. Partitions can only grow, and growing them changes which partition each
key maps to.

### Check broker support before newer APIs

Methods tied to newer brokers throw `KafkaServerDoesNotSupportApiKey` on older ones:
`describeTopicPartitions` (4.0+), `describeConsumerGroups` (4.0+, KIP-848 groups),
share-group methods (4.1+), `describeTransactions` and `describeProducers` (3.0+), KRaft quorum
methods (3.6+/3.7+). Use `describeClassicGroups` for classic groups.

For ACLs, SCRAM, delegation tokens, transactions, features, and controller bootstrap, read
[admin operations](references/operations.md).

## Common Mistakes

- **CRITICAL — Deleting or reconfiguring topics in a shared cluster from an unattended script
  without a guard.** `deleteTopics` is irreversible. Require an explicit allow-list or a dry-run
  flag, and use `validateOnly: true` on `createTopics`/`incrementalAlterConfigs` first.
- **HIGH — Resetting offsets while the group is running.** Running members commit over the reset.
  Stop the consumers (check `describeGroups` shows no members), then reset.
- **HIGH — `alterConfigs` instead of `incrementalAlterConfigs`.** The non-incremental API replaces
  the full config set for the resource, silently reverting keys you did not list.
- **MEDIUM — Assuming `describeTopicPartitions` returns everything.** It returns one page; loop on
  `nextCursor` yourself.
- **MEDIUM — Leaving the admin connected.** Use `await using` or `disconnect()` in `finally`.

## Completion

- `admin.listTopics()` / `describeConfigs` / `fetchOffsets` reflect the intended state.
- Destructive operations ran only against the intended cluster (`kafka.configSource()` confirms
  where `brokers` came from).

## References

- [Admin operations](references/operations.md): read when working with ACLs, SCRAM credentials,
  delegation tokens, transactions, finalized features, or KRaft controller bootstrap.
