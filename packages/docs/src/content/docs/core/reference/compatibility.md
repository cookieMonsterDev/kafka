---
title: Compatibility
description: Broker support and constructor defaults, the implemented surface, and APIs this client does not implement
order: 8
section: reference
---

`@cookiemonsterdev/kafka-core` is a TypeScript Apache Kafka client for
Node.js that speaks the Kafka wire protocol directly — no JVM.

The client negotiates protocol versions from `ApiVersions`. The support floor
is **Kafka 0.10+**. See
[Apache Kafka compatibility](https://kafka.apache.org/44/getting-started/compatibility/)
and the [protocol guide](https://kafka.apache.org/44/design/protocol/).

## Tested brokers

Integration tests cover **0.10, 0.11, 1.1, 2.4, 3.6, 4.0, 4.1, 4.2, and 4.4**
(the 3.6 stack runs `apache/kafka:3.9.2`). Default `KAFKA_VERSION` remains
**4.0**. Which versions CI runs depends on the event; see the table below.

The client talks to **4.0 and 4.4** via overlap (Produce 3–13, Fetch 4–18,
Metadata 0–13). Kafka 4.0 brokers dropped Produce v0–2 and Fetch v0–3 (KIP-896;
see
[Apache Kafka compatibility](https://kafka.apache.org/44/getting-started/compatibility/));
the client still encodes those versions for 0.10 clusters and will not send
them to 4.0 because the broker does not advertise them. Kafka 4.0 advertises
Produce through v12; v13 (topic IDs, KIP-516) is used when the broker
advertises it and Cluster metadata includes a `topicId`. Kafka 4.0 advertises
Fetch through v17; v18 (KIP-1166) is used on 4.1+. Fetch v13+ (topic IDs,
KIP-516) is used when the broker advertises it and Cluster metadata includes a
`topicId`.

Metadata v10–v13 decode KIP-516 topic IDs (`topicId` as a 16-byte `Buffer` on
each topic). Produce v13 and Fetch v13+ address topics by those IDs; earlier
versions still use topic names. `admin.describeTopicPartitions`
(key 75, Kafka 4.0+) also returns `topicId` on each described topic.

| `KAFKA_VERSION`      | CI runs                                          |
| -------------------- | ------------------------------------------------ |
| 3.6, 4.0, 4.4        | Every run, including pull requests               |
| 4.1, 4.2             | Pushes to `develop` and `master`, weekly, manual |
| 0.10, 0.11, 1.1, 2.4 | Weekly scheduled run and manual runs only        |

## Constructor defaults

These defaults are product choices.

| Setting               | This client                                                                                                                                                                   |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `idempotent`          | `false` — explicit opt-in for idempotent producing                                                                                                                            |
| `readUncommitted`     | `false` (isolation defaults to `read_committed`)                                                                                                                              |
| `lingerMs`            | `5`; `0` adds no delay, but calls made in the same event-loop turn still share a Produce request ([details](../producer/#linger-0-and-same-turn-sends))                       |
| `batchSize`           | `16384`; pass `0` to not batch by size                                                                                                                                        |
| `maxInFlightRequests` | `5` on the producer; pass `null` to uncap. Consumer/admin connections stay uncapped unless set                                                                                |
| Partitioner           | murmur2 by default; KIP-794 `Partitioners.StickyPartitioner` is opt-in (the preset enables it). Once enabled, its latency-adaptive rotation defaults to on (`adaptive: true`) |
| Partition assigner    | round-robin (`PartitionAssigners.roundRobin`); range, sticky, and cooperative-sticky are opt-in                                                                               |
| Compression           | GZIP, Snappy, LZ4, and ZSTD are built in (overridable via `CompressionCodecs`). GZIP/ZSTD use the zlib threadpool; Snappy/LZ4 run off-thread                                  |

See [producer configs](https://kafka.apache.org/44/configuration/producer-configs/)
and [consumer configs](https://kafka.apache.org/44/configuration/consumer-configs/) for
the equivalent broker-facing property names.

The opt-in sticky partitioner keeps unkeyed records on one partition for each
Produce batch formed by this client's `lingerMs` / `batchSize` model, then
rotates uniformly to a different available partition. Explicit partitions and
keyed murmur2 routing are unchanged.

## Throughput preset

`throughputPreset()` is a named profile for load-oriented clients. Constructor
defaults stay as in the table above. Spread the returned fragments:

```ts
import { Kafka, throughputPreset } from '@cookiemonsterdev/kafka-core';

const kafka = new Kafka({ clientId: 'load', brokers: ['localhost:9092'] });
const { producer, consumer } = throughputPreset();
const p = kafka.producer({ ...producer });
await kafka.consumer({ groupId: 'load' }).run({
  ...consumer,
  eachBatch: async ({ batch }) => {
    for (const message of batch.messages) {
      void message;
    }
  },
});
```

`producer` sets the sticky partitioner and a 32 MiB `bufferMemory`. Linger,
batch size, and in-flight caps are already constructor defaults (`lingerMs: 5`,
`batchSize: 16384`, `maxInFlightRequests: 5`). `consumer` sets
`partitionsConsumedConcurrently: 4` on `run()` (not on `kafka.consumer()`).

`eachBatch` plus `partitionsConsumedConcurrently` is the heavy-load consume
API. Compression: GZIP and
ZSTD already use the zlib threadpool; Snappy and LZ4 are off-thread as well
(optional native packages if installed).

Full walkthrough: [Throughput](../../guides/throughput/).

## Implemented surface

Method lists: [Producer API](../producer/), [Consumer API](../consumer/),
[Admin API](../admin/). The notes below are version floors and protocol keys,
not a backlog.

**Misrouted connections.** ApiVersions (key 18) v0–v5. On Kafka 4.4+ (v5, KIP-1242),
when the client connects to a broker known from metadata, it sends the cluster id and
node id it expects, so a broker reached through a reused hostname or IP answers
`REBOOTSTRAP_REQUIRED`. The client closes that connection, rediscovers the cluster
from the bootstrap brokers as with KIP-1102, and looks the broker up again.
Bootstrap connections send no ids. The check needs `metadataRecovery: 'rebootstrap'`
(the default); with `'none'` no ids are sent, as in the Java client with
`metadata.recovery.strategy=none`. Older brokers are not checked: they reject v5 and the
client falls back to the highest version they support.

**Coordinators.** Group and transaction coordinators are found with
FindCoordinator (key 10) v0–v5. On Kafka 3.0+ (v4, KIP-699) one request finds
every coordinator an admin call needs, such as `admin.describeGroups`,
`admin.deleteGroups`, or `admin.describeTransactions` over many IDs; an older
broker gets one request per distinct ID. v5 (Kafka 3.8+) tells the broker the client
handles `TRANSACTION_ABORTABLE` (KIP-890). v6 (share coordinator keys, KIP-932, which
only brokers look up) is not sent.

**Transactions.** The transactional producer sends InitProducerId (key 22) v0–v5,
AddPartitionsToTxn (24) v0–v3, AddOffsetsToTxn (25) v0–v4, EndTxn (26) v0–v5, and
TxnOffsetCommit (28) v0–v6. On Kafka 3.8+ these versions tell the broker the client
handles `TRANSACTION_ABORTABLE` (KIP-890 part 1). A transaction that fails with it can
only be aborted, as after any other failed send, `sendOffsets`, or rejected `commit()`;
after `abort()` the producer begins the next transaction as usual. If the broker rejects
the `abort()` itself with anything but a fencing error, the transaction stays open for
another `abort()`, where the Java client fails the producer. AddPartitionsToTxn
v4 and later batch transactions for checks between brokers, and the broker accepts them
only from other brokers, so the producer stays on v3, as the Java client does.

Transaction version 2 (KIP-890 part 2) is used when the cluster has finalized the
`transaction.version` feature at 2 or higher. New Kafka 4.0+ clusters do this by default.
Under it, Produce v12+ adds partitions to the transaction and TxnOffsetCommit v5+ adds the
group's offsets, so the producer sends no AddPartitionsToTxn or AddOffsetsToTxn. Each
EndTxn v5 bumps the producer epoch and returns the producer id and epoch for the next
transaction, so the producer sends InitProducerId only before its first transaction.
Brokers advertise these versions whatever the feature level is. With
`transaction.version` below 2, a transactional Produce stays on v11 or lower, and EndTxn
and TxnOffsetCommit stay on v4 or lower, because the broker reads the newer versions as
transaction version 2. The producer reads the feature level from the same ApiVersions
response as the broker's API versions, when it first connects, and keeps it while it
reuses those versions; create a new producer to pick up a change. If a producer does read
a new level that turns transaction version 2 on, it bumps its epoch before its first
transaction under it, as the Java client does.

On Kafka 4.4+ (TxnOffsetCommit v6, KIP-1319), `sendOffsets` addresses each topic by its
topic id. Like the Java client, the producer sends v6 only under transaction version 2 and
only when its metadata has an id for every topic. Before it sends the offsets to a cluster
that supports v6, it adds their topics to the metadata it keeps; if that lookup fails, or a
topic has no id, the offsets go by topic name on v5. `UNKNOWN_TOPIC_ID` refreshes the
metadata before the retry. v6 can also answer `GROUP_ID_NOT_FOUND` or `STALE_MEMBER_EPOCH`
where older versions answer `ILLEGAL_GENERATION`; as with any failed `sendOffsets`, the
transaction can then only be aborted. The producer sends offsets without a member id or
generation, which the coordinator accepts without checking the group's members, so in
practice neither error occurs.

**Offset lookups.** ListOffsets (key 2) v0–v11. `admin.fetchTopicOffsets`
takes an `offsetSpec` from `OffsetSpecs`: max timestamp (v7, Kafka 3.0), earliest
local offset (v8, 3.5), latest tiered offset (v9, 3.9, KIP-1005), and earliest
pending upload offset (v11, 4.2, KIP-1023). A spec the broker is too old for
throws `KafkaServerDoesNotSupportApiKey`. The same check covers
`admin.fetchTopicOffsetsByTimestamp` and `kafka topic offsets --time max-timestamp`:
on a broker older than 3.0, the max timestamp lookup throws instead of being read as
a timestamp. From v10 (Kafka 4.0, KIP-1075) the request carries the client's
`requestTimeout` as the broker's limit for reading tiered storage.

**Committed offsets.** OffsetCommit (key 8) v0–v10 and OffsetFetch (key 9) v1–v10.
On Kafka 3.0+ (OffsetFetch v8, KIP-709) `admin.listConsumerGroupOffsets` sends one
request per group coordinator for all of its groups; an older broker gets one request
per group. From Kafka 3.7 (OffsetFetch v9, KIP-848) a consumer with
`groupProtocol: 'consumer'` sends its member id and member epoch when it fetches
committed offsets, so the coordinator rejects a stale member. On Kafka 4.2+ (v10 of
both) commits and fetches address each topic by its topic id when the client's
metadata has one; a request with a topic that has none, or an offset fetch for every
topic of a group, stays on v9. `UNKNOWN_TOPIC_ID` refreshes the metadata before the
retry. `STALE_MEMBER_EPOCH` is retriable here, unlike in the Java client: the commit
or fetch is sent again with the epoch the member's next heartbeat brought.

**Consumer.** Range, RoundRobin, Sticky, and CooperativeSticky are built in
(`PartitionAssigners`). The default assigner is still round-robin. Classic
JoinGroup/SyncGroup remains the default membership protocol. Fetch uses
incremental **fetch sessions** (KIP-227) on Kafka 2.3+; older brokers stay
sessionless (`sessionId = 0`). Set
`groupProtocol: 'consumer'` (broker property `group.protocol`) to opt into KIP-848
ConsumerGroupHeartbeat on Kafka 4.0+; assignment is server-side and
incremental. `admin.describeConsumerGroups` uses ConsumerGroupDescribe
(key 69) via each group coordinator on Kafka 4.0+. `admin.describeClassicGroups`
is an alias for `admin.describeGroups` (DescribeGroups, key 15) for classic
JoinGroup groups. `fromBeginning` is boolean (earliest vs latest). `autoOffsetReset: 'none'`
throws if there is no committed offset. `autoOffsetReset: 'by_duration:PT1H'` (KIP-1106)
starts at the first offset at or after `now` minus that ISO-8601 duration. Cooperative-sticky
uses KIP-429 incremental revoke semantics and performs the follow-up generation
needed to settle partitions that move between members. This assignor support
applies to the classic group protocol. `kafka.shareConsumer()` implements
KIP-932 share groups (ShareGroupHeartbeat / ShareFetch / ShareAcknowledge,
keys 76–79) on Kafka 4.1+. ShareFetch and ShareAcknowledge negotiate v1–v2;
v2 adds `shareAcquireMode` (KIP-1206) and `RENEW` acknowledgements (KIP-1222)
on Kafka 4.2+. Classic `consumer()` remains the default.

**Admin.** `admin.alterConfigs` is kept for older brokers. Prefer
`admin.incrementalAlterConfigs` (key 44). `admin.electLeaders` is key 43
(historically ElectPreferredLeaders). `admin.deleteGroupOffsets` is OffsetDelete
(key 47). `admin.describeUserScramCredentials` and `admin.alterUserScramCredentials`
are keys 50–51. `admin.describeClientQuotas` / `admin.alterClientQuotas` are
keys 48–49. `admin.describeLogDirs` / `admin.alterReplicaLogDirs` are keys
34–35. DescribeLogDirs negotiates v0–v5: v3 adds a request-level error code, so
a broker that refuses the request (such as `CLUSTER_AUTHORIZATION_FAILED`) now
makes `describeLogDirs` throw where older versions returned no log dirs; v4
(Kafka 3.3+, KIP-827) adds each log dir's `totalBytes` and `usableBytes`; v5
(Kafka 4.3+) adds `isCordoned`. Fields the broker does not send are left out, and
a volume size of `-1` (unreadable) is left out too. `admin.describeReplicaLogDirs`
filters DescribeLogDirs (35) to specific broker/replica pairs.
`admin.alterPartitionReassignments` implements AlterPartitionReassignments
(key 45) v0–v1. `allowReplicationFactorChange: false` (KIP-860) needs v1
(Kafka 4.1+); on an older broker it throws instead of being ignored. `admin.describeCluster` uses DescribeCluster (key 60) when advertised
and Metadata otherwise. `admin.describeProducers` uses key 61 on Kafka 3.0+
and queries partition leaders unless a `brokerId` is supplied.
`admin.describeTransactions` uses key 65, dynamically discovers transaction
coordinators, and requires Kafka 3.0+. `admin.listTransactions` uses key 66,
fans the request out to every broker, unique-merges by transactional ID, and
requires Kafka 3.0+; v1 adds `durationFilter` and v2 adds
`transactionalIdPattern`. `admin.fenceProducers` uses InitProducerId (22) via
transaction coordinators (Kafka 2.5+ / v3+). `admin.abortTransaction` uses
WriteTxnMarkers (27) on partition leaders (Kafka 3.0+ / v1+; v0 removed in
4.0). `admin.forceTerminateTransaction` fences one transactional ID via
InitProducerId. `admin.describeFeatures` reads
ApiVersions (18) v3+ tagged fields (KIP-584) from the active controller.
`admin.describeApiVersions` sends ApiVersions (18) to every broker, on any broker version,
and reports the version the client negotiates for each API (KIP-1220).
`admin.removeMembersFromConsumerGroup` uses LeaveGroup (13) v3+ with explicit
member identities. `admin.describeConsumerGroups` uses ConsumerGroupDescribe
(key 69) via group coordinators and requires Kafka 4.0+. `admin.describeClassicGroups`
aliases `admin.describeGroups` (DescribeGroups, key 15) for classic JoinGroup
groups. DescribeGroups negotiates v0–v6: on v6 (Kafka 4.0+, KIP-1043) an unknown group id,
or one that is not a classic group, makes `admin.describeGroups` throw (`cause.type` is
`GROUP_ID_NOT_FOUND`) where older versions returned an empty `Dead` group;
`consumer.describeGroup()` still returns the `Dead` group. `admin.listGroups` implements ListGroups
(key 16) v0–v5; its `types` filter (KIP-848) needs v5 (Kafka 3.8+) and throws on an older
broker instead of being ignored. `admin.deleteGroups` implements DeleteGroups (key 42)
v0–v3; on v3 (Kafka 4.4+) a failed group's `error` carries the broker's message when it sends
one, such as the cause of `GROUP_DELETION_FAILED`. `admin.describeShareGroups`, `listShareGroupOffsets`,
`alterShareGroupOffsets`, `deleteShareGroupOffsets`, and `deleteShareGroups`
implement share-group Admin (keys 77, 90–92, plus DeleteGroups 42) on Kafka 4.1+.
`admin.describeStreamsGroups` implements StreamsGroupDescribe (key 89) v0–v1 (KIP-1071) on
Kafka 4.2+; v1 (Kafka 4.4+) adds topology descriptions (KIP-1331) and the assignor name, and
`includeTopologyDescription` throws on an older broker instead of being ignored. The client
does not implement StreamsGroupHeartbeat, so it cannot join a streams group.
`admin.describeTopicPartitions` uses
key 75 on Kafka 4.0+ (KIP-966), sends topic names, and returns one page plus
`nextCursor` for the caller to continue. `admin.updateFeatures` implements
UpdateFeatures (key 57) v0–v2 and targets the active controller; v0 cannot
validate-only and rejects unsafe downgrades. `admin.describeMetadataQuorum`
implements DescribeQuorum (key 55) v0–v2 against the active controller and requires
KRaft 3.6+. v1 adds replica timestamps (KIP-836); v2 adds directory IDs and
node listeners (KIP-853). `admin.unregisterBroker` implements UnregisterBroker (key 64) v0.
`admin.unregisterController` implements UnregisterController (key 94) v0 (KIP-1312) on
Kafka 4.4+ with `metadata.version` 4.4-IV2 or later.
`admin.assignReplicasToDirs` implements AssignReplicasToDirs (key 73) v0 (KIP-858)
and assigns replicas on a broker to log directories by 16-byte directory UUID.
`admin.addRaftVoter` and `admin.removeRaftVoter` implement keys 80–81; v1 of
AddRaftVoter adds optional `ackWhenCommitted` (default `true`). These controller
RPCs require KRaft 3.7+ when the broker advertises the API. `admin.listConfigResources`
implements ListConfigResources (key 74) v0–v1 and targets the active
controller. v0 lists client metrics names only (Kafka 4.0); filtering by
`resourceTypes` needs v1 (Kafka 4.1+ / KIP-1142). An empty `resourceTypes`
list is valid: v1 returns the broker's default supported types, v0 returns
all client metrics. `admin.createDelegationToken`,
`admin.renewDelegationToken`, `admin.expireDelegationToken`, and
`admin.describeDelegationToken` implement keys 38–41 (Kafka 1.1+).

**Security.** SASL PLAIN, SCRAM, OAUTHBEARER, and GSSAPI / Kerberos are
implemented. GSSAPI is opt-in (`mechanism: 'gssapi'`): supply `gssProvider` or
install the optional `kerberos` package; you still need a KDC and a ticket or
keytab. CI does not run a Kerberos stack. The `aws` SASL helper is extra
(non-Apache). Admin can create, describe, renew, and expire delegation tokens.
SASL login with a delegation token is opt-in: set `sasl.mechanism` to
`scram-sha-256` or `scram-sha-512` and pass `tokenId` / `tokenHmac` (see
[Security](../../guides/security/)). The broker still needs
`delegation.token.secret.key` and SASL/SCRAM. See
[SASL authentication](https://kafka.apache.org/44/security/authentication-using-sasl/)
and the [security guide](../../guides/security/).

## Not implemented

These are not in `@cookiemonsterdev/kafka-core`. Broker-internal RPCs
(controller replication, share-group state keys 83–87, and similar) are not
client APIs and are omitted here.

**Packages.** No Kafka Streams or Kafka Connect packages. See
[Kafka Streams](https://kafka.apache.org/44/streams/introduction/) and
[Kafka Connect](https://kafka.apache.org/44/kafka-connect/overview/).

**Streams groups (KIP-1071).** No StreamsGroupHeartbeat or StreamsGroupDescribe
(keys 88–89). Admin has no `describeStreamsGroups`, `listStreamsGroupOffsets`,
`alterStreamsGroupOffsets`, `deleteStreamsGroupOffsets`, or
`deleteStreamsGroups`.

**Consumer shape.** Membership is `run()` / `stream()`, not a blocking `poll()`
loop. Offsets as `bigint`, MessageSet, ZSTD, and `KAFKA_*` env vars:
[Breaking changes](../../migration/breaking-changes/).
