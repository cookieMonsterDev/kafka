---
title: Admin
description: Topics, configs, offsets, groups, ACLs, and SCRAM
order: 3
section: guides
---

```ts
import { Kafka } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092'] });
const admin = kafka.admin();
await admin.connect();
await admin.createTopics({
  topics: [{ topic: 'events', numPartitions: 3, replicationFactor: 1 }],
});
await admin.disconnect();
```

Full method list: [`Admin`](../reference/admin/). Source:
[`admin/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/admin/types.ts).
Cluster operations:
[Basic Kafka operations](https://kafka.apache.org/43/operations/basic-kafka-operations/).

## Topics

`createTopics`, `deleteTopics`, `listTopics`, `createPartitions`,
`fetchTopicMetadata`. Offset fields from `fetchTopicOffsets` and
`fetchOffsets` are `bigint`.

## Configs

Prefer `incrementalAlterConfigs` (Kafka 2.3+). `alterConfigs` remains for older
brokers. See [topic configs](https://kafka.apache.org/43/configuration/topic-configs/).

## Groups, ACLs, SCRAM

`listGroups` / `describeGroups` / `deleteGroups` / `deleteGroupOffsets`.
ACL helpers use `AclResourceTypes`, `AclOperationTypes`, `AclPermissionTypes`,
and `ResourcePatternTypes`. SCRAM:
`describeUserScramCredentials` / `alterUserScramCredentials`.

## Transactions

Kafka 3.0+ supports transaction inspection through API key 65:

```ts
const { transactionStates } = await admin.describeTransactions(['payments-writer']);
for (const transaction of transactionStates) {
  console.log(transaction.transactionalId, transaction.transactionState);
}
```

The client discovers and groups requests by transaction coordinator. Producer
IDs and transaction start times are returned as `bigint`.

`electLeaders`, `describeCluster`, log dirs, and quotas are implemented.

Kafka 3.0+ can report the producer state retained by partition leaders:

```ts
const producerStates = await admin.describeProducers({
  topicPartitions: [{ topic: 'events', partitions: [0, 1] }],
});

for (const state of producerStates) {
  console.log(state.topic, state.partition, state.activeProducers);
}
```

Pass `brokerId` to query a specific replica instead.

## Finalized features

`updateFeatures` targets the active controller. Prefer `validateOnly: true`
before changing a feature level, especially for `metadata.version`:

```ts
import { FeatureUpdateUpgradeTypes } from '@cookiemonsterdev/kafka-core';

await admin.updateFeatures({
  featureUpdates: [
    {
      feature: 'metadata.version',
      maxVersionLevel: 20,
      upgradeType: FeatureUpdateUpgradeTypes.SAFE_DOWNGRADE,
    },
  ],
  validateOnly: true,
});
```

Upgrade types are `UPGRADE`, `SAFE_DOWNGRADE`, and `UNSAFE_DOWNGRADE`.
UpdateFeatures v0 supports upgrades and safe downgrades, but rejects unsafe
downgrades and `validateOnly`; newer brokers negotiate v1 or v2 automatically.

Remaining transaction administration APIs are listed under
[Compatibility](../reference/compatibility/).
