---
title: Admin
description: Topics, configs, offsets, groups, ACLs, and SCRAM
order: 3
section: guides
---

```ts
import { Kafka } from '@kafka/core';

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

`electLeaders`, `describeCluster`, log dirs, and quotas are implemented.
Missing APIs (describeProducers, transaction describe) are listed under
[Compatibility](../reference/compatibility/).
