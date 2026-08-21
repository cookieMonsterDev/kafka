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

| Method                                                               | Notes                                            |
| -------------------------------------------------------------------- | ------------------------------------------------ |
| `listTopics()`                                                       |                                                  |
| `createTopics({ topics, validateOnly?, timeout?, waitForLeaders? })` | `validateOnly` needs CreateTopics v1 (0.11+)     |
| `deleteTopics({ topics, timeout? })`                                 |                                                  |
| `createPartitions({ topicPartitions, validateOnly?, timeout? })`     |                                                  |
| `fetchTopicMetadata({ topics? })`                                    | Optional `topicId` (`Buffer`) when Metadata v10+ |
| `deleteTopicRecords({ topic, partitions })`                          |                                                  |

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
| `describeProducers({ topicPartitions, brokerId? })`            | DescribeProducers (key 61), Kafka 3.0+   |
| `electLeaders({ topicPartitions?, electionType?, timeout? })`  | Key 43                                   |
| `alterPartitionReassignments` / `listPartitionReassignments`   |                                          |
| `updateFeatures({ featureUpdates, validateOnly?, timeout? })`  | Key 57; KRaft feature levels             |

`describeProducers` queries each partition leader by default. Set `brokerId` to inspect a
specific replica. It returns one entry per partition with `activeProducers`; producer IDs,
timestamps, and transaction start offsets use `bigint`, and
`currentTransactionStartOffset` is `null` when no transaction is open.

## Transactions

`describeTransactions(transactionalIds)` discovers each transaction coordinator
and returns `{ transactionStates }`. Each transaction state includes its
transactional ID, state, timeout, start time, producer ID and epoch, and active
topic partitions. Producer IDs and transaction start times are `bigint`.
DescribeTransactions is API key 65 and requires Kafka 3.0 or newer.

## ACLs, SCRAM, quotas, log dirs

| Method                                                       | Notes                                     |
| ------------------------------------------------------------ | ----------------------------------------- |
| `createAcls` / `describeAcls` / `deleteAcls`                 | Prefixed patterns need ACL APIs v1 (2.0+) |
| `describeUserScramCredentials` / `alterUserScramCredentials` | Keys 50–51                                |
| `describeClientQuotas` / `alterClientQuotas`                 | Keys 48–49                                |
| `describeLogDirs` / `alterReplicaLogDirs`                    | Keys 34–35                                |

Also `connect`, `disconnect`, `logger()`, `Symbol.asyncDispose`. Missing
methods: [Compatibility](./compatibility/).
