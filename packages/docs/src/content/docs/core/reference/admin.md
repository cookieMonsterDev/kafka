---
title: Admin API
description: Implemented admin methods
order: 4
section: reference
---

Returned by `kafka.admin()`. Source:
[`admin/types.ts`](https://github.com/cookieMonsterDev/kafka/blob/master/packages/core/src/admin/types.ts).
Guide: [Admin](../../guides/admin/). Apache:
[basic operations](https://kafka.apache.org/43/operations/basic-kafka-operations/).

Offset inputs (`seek`, `deleteTopicRecords`, `setOffsets`) accept
`bigint | number | string`. Responses use `bigint`.

## Topics

| Method                                                                  | Notes                                            |
| ----------------------------------------------------------------------- | ------------------------------------------------ |
| `listTopics()`                                                          |                                                  |
| `createTopics({ topics, validateOnly?, timeout?, waitForLeaders? })`    | `validateOnly` needs CreateTopics v1 (0.11+)     |
| `deleteTopics({ topics, timeout? })`                                    |                                                  |
| `createPartitions({ topicPartitions, validateOnly?, timeout? })`        |                                                  |
| `fetchTopicMetadata({ topics? })`                                       | Optional `topicId` (`Buffer`) when Metadata v10+ |
| `describeTopicPartitions({ topics, responsePartitionLimit?, cursor? })` | DescribeTopicPartitions (key 75), Kafka 4.0+     |
| `deleteTopicRecords({ topic, partitions })`                             |                                                  |

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
| `describeClassicGroups(ids)`                                   | DescribeGroups (15); classic JoinGroup   |
| `describeConsumerGroups(ids)`                                  | ConsumerGroupDescribe (69), Kafka 4.0+   |
| `describeShareGroups(ids)`                                     | ShareGroupDescribe (77), Kafka 4.1+      |
| `listShareGroupOffsets({ groups })`                            | DescribeShareGroupOffsets (90)           |
| `alterShareGroupOffsets({ groupId, topics })`                  | AlterShareGroupOffsets (91)              |
| `deleteShareGroupOffsets({ groupId, topics })`                 | DeleteShareGroupOffsets (92)             |
| `deleteShareGroups(ids)`                                       | DeleteGroups (42) for share groups       |
| `removeMembersFromConsumerGroup({ groupId, members })`         | LeaveGroup (13) v3+; per-member errors   |
| `describeConfigs` / `alterConfigs` / `incrementalAlterConfigs` | Prefer incremental                       |
| `listConfigResources({ resourceTypes? })`                      | Key 74; empty types lists defaults       |
| `describeCluster()`                                            | DescribeCluster (key 60) when advertised |
| `describeProducers({ topicPartitions, brokerId? })`            | DescribeProducers (key 61), Kafka 3.0+   |
| `electLeaders({ topicPartitions?, electionType?, timeout? })`  | Key 43                                   |
| `alterPartitionReassignments` / `listPartitionReassignments`   |                                          |
| `updateFeatures({ featureUpdates, validateOnly?, timeout? })`  | Key 57; KRaft feature levels             |
| `describeFeatures()`                                           | ApiVersions (18) v3+ tags; KRaft 3.6+    |
| `describeMetadataQuorum()`                                     | DescribeQuorum (key 55); KRaft 3.6+      |
| `unregisterBroker({ brokerId })`                               | UnregisterBroker (key 64); KRaft 3.7+    |
| `addRaftVoter({ voterId, voterDirectoryId, listeners, ... })`  | AddRaftVoter (key 80); KRaft 3.7+        |
| `removeRaftVoter({ voterId, voterDirectoryId, ... })`          | RemoveRaftVoter (key 81); KRaft 3.7+     |

`describeConsumerGroups` discovers each group coordinator and sends
ConsumerGroupDescribe (key 69). Use it for KIP-848 `groupProtocol: 'consumer'`
groups on Kafka 4.0+. `describeClassicGroups` is an alias for `describeGroups`
(DescribeGroups, key 15) for classic JoinGroup/SyncGroup groups.

`describeProducers` queries each partition leader by default. Set `brokerId` to inspect a
specific replica. It returns one entry per partition with `activeProducers`; producer IDs,
timestamps, and transaction start offsets use `bigint`, and
`currentTransactionStartOffset` is `null` when no transaction is open.

`describeTopicPartitions` is name-based (optional `topicId` on input is accepted). It
returns `{ topics, nextCursor }` for a single page; pass `nextCursor` to continue.
Each topic includes `topicId` as a 16-byte `Buffer`. Produce and Fetch still use names.

`describeMetadataQuorum()` sends DescribeQuorum (key 55) for the
`__cluster_metadata` partition to the active controller and returns metadata quorum
partition state (v0–v2). `highWatermark` and `logEndOffset` values are `bigint`.
v1 adds replica fetch timestamps; v2 adds `errorMessage`, `replicaDirectoryId`,
and controller `nodes`. `voterDirectoryId` for raft voter APIs is a
16-byte `Buffer`.

## Transactions

`describeTransactions(transactionalIds)` discovers each transaction coordinator
and returns `{ transactionStates }`. Each transaction state includes its
transactional ID, state, timeout, start time, producer ID and epoch, and active
topic partitions. Producer IDs and transaction start times are `bigint`.
DescribeTransactions is API key 65 and requires Kafka 3.0 or newer.

`listTransactions(options?)` sends ListTransactions (API key 66) to every broker
and unique-merges listings by transactional ID. Each listing is
`{ transactionalId, producerId, transactionState }`; producer IDs are `bigint`.
Omit filters (or pass empty arrays) to list all transactions the coordinators
know about. Optional filters:

| Option                   | Notes                                                                  |
| ------------------------ | ---------------------------------------------------------------------- |
| `stateFilters`           | Transaction states such as `Ongoing` or `Empty`                        |
| `producerIdFilters`      | `bigint[]`; empty means all producers                                  |
| `durationFilter`         | v1+ (Kafka 3.5+). Milliseconds; omit or `-1n` means no duration filter |
| `transactionalIdPattern` | v2+ regex. `null` or omitted means no pattern filter                   |

On brokers that only speak v0, the client omits v1/v2 fields rather than
sending them. ListTransactions requires Kafka 3.0 or newer.

`fenceProducers({ transactionalIds, transactionTimeout? })` sends InitProducerId
(key 22) with `producerId: -1n` and `producerEpoch: -1` to each transaction
coordinator. Returns `{ results }` with per-ID `errorCode`, and on success
`producerId` / `producerEpoch` as `bigint` / `number`. Requires Kafka 2.5+
(InitProducerId v3+). Default `transactionTimeout` is 60_000 ms.

`abortTransaction({ topic, partition, producerId, producerEpoch, coordinatorEpoch?, transactionVersion? })`
sends WriteTxnMarkers (key 27) with `transactionResult: false` to the partition
leader. Omit `coordinatorEpoch` to resolve it from `describeProducers` on that
partition. Requires Kafka 3.0+ (WriteTxnMarkers v1+; v0 removed in 4.0). v2
adds optional `transactionVersion` when the broker negotiates WriteTxnMarkers v2.

`forceTerminateTransaction({ transactionalId, transactionTimeout? })` fences a
single transactional producer via InitProducerId, a convenience wrapper
around `fenceProducers`. Returns `{ transactionalId, errorCode, ... }`.

## ACLs, SCRAM, quotas, log dirs

| Method                                                       | Notes                                      |
| ------------------------------------------------------------ | ------------------------------------------ |
| `createAcls` / `describeAcls` / `deleteAcls`                 | Prefixed patterns need ACL APIs v1 (2.0+)  |
| `describeUserScramCredentials` / `alterUserScramCredentials` | Keys 50–51                                 |
| `describeClientQuotas` / `alterClientQuotas`                 | Keys 48–49                                 |
| `describeLogDirs` / `alterReplicaLogDirs`                    | Keys 34–35                                 |
| `describeReplicaLogDirs(replicas)`                           | DescribeLogDirs filtered by broker/replica |

## Tokens

`createDelegationToken`, `describeDelegationToken`, `renewDelegationToken`,
and `expireDelegationToken` are keys 38–41 (Kafka 1.1+). They target the
active controller. HMAC values are `Buffer`; issue, expiry, and max timestamps
are `bigint`. Owner and renewer principals are `{ principalType, name }`
(`User` + name).

`createDelegationToken({ owner })` needs CreateDelegationToken v3 (Kafka 3.3+).
`expireDelegationToken({ hmac, expiryTimePeriodMs: -1n })` expires immediately.
Brokers must set `delegation.token.secret.key` and accept the
request over SASL; PLAINTEXT returns `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`.
Default integration compose files do not enable tokens. Pass the returned
`tokenId` and `hmac` as `sasl.tokenId` / `sasl.tokenHmac` on a SCRAM client to
authenticate with the token — see [Security](../../guides/security/).

Also `connect`, `disconnect`, `logger()`, `on` / `events`, `Symbol.asyncDispose`.
APIs this client does not implement are listed under
[Compatibility](./compatibility/#not-implemented).
