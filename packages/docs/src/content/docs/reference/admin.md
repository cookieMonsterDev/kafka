---
title: Admin API
description: Implemented admin methods
order: 4
section: reference
---

Returned by `kafka.admin()`. Source:
[`admin/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/admin/types.ts).
Guide: [Admin](../guides/admin/). Apache:
[basic operations](https://kafka.apache.org/43/operations/basic-kafka-operations/).

Offset inputs (`seek`, `deleteTopicRecords`, `setOffsets`) accept
`bigint | number | string`. Responses use `bigint`.

## Topics

| Method                                                               | Notes                                        |
| -------------------------------------------------------------------- | -------------------------------------------- |
| `listTopics()`                                                       |                                              |
| `createTopics({ topics, validateOnly?, timeout?, waitForLeaders? })` | `validateOnly` needs CreateTopics v1 (0.11+) |
| `deleteTopics({ topics, timeout? })`                                 |                                              |
| `createPartitions({ topicPartitions, validateOnly?, timeout? })`     |                                              |
| `fetchTopicMetadata({ topics? })`                                    |                                              |
| `deleteTopicRecords({ topic, partitions })`                          |                                              |

## Offsets

| Method                                                | Notes                               |
| ----------------------------------------------------- | ----------------------------------- |
| `fetchTopicOffsets(topic)`                            | `offset`, `high`, `low` as `bigint` |
| `fetchTopicOffsetsByTimestamp(topic, timestamp?)`     |                                     |
| `fetchOffsets({ groupId, topics?, resolveOffsets? })` |                                     |
| `setOffsets({ groupId, topic, partitions })`          |                                     |
| `resetOffsets({ groupId, topic, earliest? })`         |                                     |
| `deleteGroupOffsets({ groupId, topics })`             | OffsetDelete (key 47)               |

## Groups, configs, cluster

| Method                                                         | Notes                                    |
| -------------------------------------------------------------- | ---------------------------------------- |
| `listGroups()` / `describeGroups(ids)` / `deleteGroups(ids)`   |                                          |
| `describeConfigs` / `alterConfigs` / `incrementalAlterConfigs` | Prefer incremental                       |
| `describeCluster()`                                            | DescribeCluster (key 60) when advertised |
| `electLeaders({ topicPartitions?, electionType?, timeout? })`  | Key 43                                   |
| `alterPartitionReassignments` / `listPartitionReassignments`   |                                          |

## ACLs, SCRAM, quotas, log dirs

| Method                                                       | Notes                                     |
| ------------------------------------------------------------ | ----------------------------------------- |
| `createAcls` / `describeAcls` / `deleteAcls`                 | Prefixed patterns need ACL APIs v1 (2.0+) |
| `describeUserScramCredentials` / `alterUserScramCredentials` | Keys 50–51                                |
| `describeClientQuotas` / `alterClientQuotas`                 | Keys 48–49                                |
| `describeLogDirs` / `alterReplicaLogDirs`                    | Keys 34–35                                |

Also `connect`, `disconnect`, `logger()`, `Symbol.asyncDispose`. Missing
methods: [Compatibility](./compatibility/).
