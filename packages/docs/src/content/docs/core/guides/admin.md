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

Full method list: [`Admin`](../../reference/admin/). Source:
[`admin/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/admin/types.ts).
Cluster operations:
[Basic Kafka operations](https://kafka.apache.org/44/operations/basic-kafka-operations/).

## Topics

`createTopics`, `deleteTopics`, `listTopics`, `createPartitions`,
`fetchTopicMetadata`, `describeTopicPartitions`. Offset fields from
`fetchTopicOffsets` and `fetchOffsets` are `bigint`. On Kafka 2.8+ (Metadata
v10+), each topic in `fetchTopicMetadata` may include `topicId` as a 16-byte
`Buffer`. Newer versions of Produce, Fetch, OffsetCommit, and OffsetFetch send
that id in place of the topic name; see
[Compatibility](../../reference/compatibility/).

Kafka 4.0+ can page partition metadata with DescribeTopicPartitions (key 75):

```ts
const { topics, nextCursor } = await admin.describeTopicPartitions({
  topics: ['events'],
  responsePartitionLimit: 2000,
});

for (const topic of topics) {
  console.log(topic.name, topic.topicId, topic.partitions.length);
}

if (nextCursor) {
  const page = await admin.describeTopicPartitions({
    topics: ['events'],
    cursor: nextCursor,
  });
  console.log(page.topics);
}
```

The method returns **one page**. Pass `nextCursor` back to continue; it does
not loop internally. Topics are selected by name (optional `topicId` on input
is accepted and ignored on the wire).

### Tiered storage offsets

With [tiered storage](https://kafka.apache.org/44/operations/tiered-storage/),
`fetchTopicOffsets` can also report where each partition's data sits. Pass an
`offsetSpec` from `OffsetSpecs`; `offset` then holds that offset next to `high`
and `low`:

```ts
import { OffsetSpecs } from '@cookiemonsterdev/kafka-core';

const offsets = await admin.fetchTopicOffsets('events', { offsetSpec: OffsetSpecs.LATEST_TIERED });
for (const { partition, offset } of offsets) {
  console.log(partition, offset === -1n ? 'nothing in tiered storage' : offset);
}
```

`EARLIEST_LOCAL` (Kafka 3.5+) is the first offset on the leader's local disk,
`LATEST_TIERED` (3.9+) the last offset copied to tiered storage, and
`EARLIEST_PENDING_UPLOAD` (4.2+) the first offset not copied yet. Without tiered
storage, `EARLIEST_LOCAL` equals `low` and the other two are `-1n`. An older
broker throws `KafkaServerDoesNotSupportApiKey`. On Kafka 4.0+ the broker waits
on tiered storage for up to the client's `requestTimeout` before it fails the
lookup with `REQUEST_TIMED_OUT`.

## Consumer group offsets

`fetchOffsets` reads one consumer group's committed offsets.
`listConsumerGroupOffsets` reads several groups at once, in the order given. A
group with no `topics` returns every topic it has committed offsets for; a
requested partition with no committed offset returns `-1n`:

```ts
const { groups } = await admin.listConsumerGroupOffsets({
  groups: [{ groupId: 'billing' }, { groupId: 'audit', topics: ['events'] }],
});
for (const { groupId, topics } of groups) {
  for (const { topic, partitions } of topics) console.log(groupId, topic, partitions);
}
```

On Kafka 3.0+ (OffsetFetch v8, KIP-709) each group coordinator answers for all
of its groups in one request; an older broker gets one request per group. A
group id may appear only once, and an error for any group, such as
`GROUP_AUTHORIZATION_FAILED`, rejects the whole call.

## Configs

Prefer `incrementalAlterConfigs` (Kafka 2.3+). `alterConfigs` remains for older
brokers. See [topic configs](https://kafka.apache.org/44/configuration/topic-configs/).

## Groups, ACLs, SCRAM

`listGroups` / `describeGroups` / `describeClassicGroups` /
`describeConsumerGroups` / `deleteGroups` / `deleteGroupOffsets`.
ACL helpers use `AclResourceTypes`, `AclOperationTypes`, `AclPermissionTypes`,
and `ResourcePatternTypes`. SCRAM:
`describeUserScramCredentials` / `alterUserScramCredentials`.

Share groups (KIP-932, Kafka 4.1+): `describeShareGroups`,
`listShareGroupOffsets`, `alterShareGroupOffsets`, `deleteShareGroupOffsets`,
`deleteShareGroups`.

## Delegation tokens

Kafka 1.1+ can mint HMAC delegation tokens through the Admin API when the
broker has `delegation.token.secret.key` and the connection is SASL:

```ts
const created = await admin.createDelegationToken({
  renewers: [{ principalType: 'User', name: 'alice' }],
  maxLifeTimeMs: 3_600_000n,
});

const { tokens } = await admin.describeDelegationToken();
await admin.renewDelegationToken({ hmac: created.hmac, renewTimePeriodMs: 1_800_000n });
await admin.expireDelegationToken({ hmac: created.hmac, expiryTimePeriodMs: -1n });
```

HMAC is `Buffer`; timestamps are `bigint`. Connect a producer or consumer
with the same token by passing `tokenId` and `tokenHmac` on
`scram-sha-256` / `scram-sha-512` — see [Security](../security/).

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

`listTransactions(options?)` fans ListTransactions (key 66) out to every
broker and unique-merges by transactional ID (Kafka 3.0+).

`fenceProducers` and `forceTerminateTransaction` fence transactional IDs via
InitProducerId (Kafka 2.5+). `abortTransaction` writes abort markers with
WriteTxnMarkers on the partition leader (Kafka 3.0+).

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

`describeFeatures()` reads ApiVersions v3+ tagged fields (KIP-584) from the
active controller.

## Cluster and KRaft

`electLeaders`, `describeCluster`, `describeLogDirs` / `alterReplicaLogDirs` /
`describeReplicaLogDirs`, and `describeClientQuotas` / `alterClientQuotas`
cover leader election, cluster metadata, log dirs, and quotas.

On KRaft clusters, `describeMetadataQuorum`, `unregisterBroker`,
`assignReplicasToDirs`, `addRaftVoter`, and `removeRaftVoter` target the active
controller. Method signatures and version floors: [Admin API](../../reference/admin/).

## Controller bootstrap

When brokers are down, Admin can talk to the KRaft controller quorum directly
(KIP-919, Kafka 3.7+). Pass controller listener addresses on this admin
instance; producer and consumer still use `KafkaConfig.brokers`.

```ts
const admin = kafka.admin({
  bootstrapControllers: ['localhost:9093'],
});
await admin.connect();
await admin.describeMetadataQuorum();
```

Discovery uses DescribeCluster with `endpointType=CONTROLLER` instead of
Metadata. This path is for quorum, voter, and other controller-targeted RPCs.
APIs that need broker-side topic metadata (`listTopics`, produce/consume) still
need a broker bootstrap list.

[bootstrap.controllers](https://kafka.apache.org/44/configuration/admin-configs/#bootstrap.controllers).
