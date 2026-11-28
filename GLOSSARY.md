# Kafka client

The domain language of this repository's Kafka client and the tools built on it. Use these words in code, docs, issues and reviews.

## Cluster and protocol

**Broker**:
One Kafka server that stores partitions and serves clients, identified in cluster metadata by its node id.
_Avoid_: server, host, node (when you mean the broker rather than its id)

**Log directory**:
A directory on a broker's disk that stores partition replicas. A broker can have several.
_Avoid_: data dir, log path

**Cordoned log directory**:
A log directory on which the broker places no new partition replicas, while the replicas already on it stay.

**Bootstrap broker**:
A broker address the client is given up front and uses only to discover the cluster.
_Avoid_: seed broker, bootstrap server

**Controller**:
The node that manages cluster metadata and applies cluster-wide changes such as creating topics. Under KRaft it is one of a quorum of controllers; under ZooKeeper it is one elected broker.
_Avoid_: master, leader

**Bootstrap controller**:
A controller address an admin client connects to directly, instead of a bootstrap broker, to manage a KRaft cluster (KIP-919).

**Misrouted connection**:
A connection that reaches a different broker, or a broker of a different cluster, than the node id and cluster the client expected, for example after a broker's hostname or IP is reused (KIP-1242).

**Metadata**:
The client's current view of the cluster: its brokers, topics, partitions and partition leaders.
_Avoid_: topology (that names a Kafka Streams processing graph), cluster state

**Coordinator**:
The broker responsible for one consumer group, share group, streams group or transactional id. Name it by its kind: group coordinator or transaction coordinator.
_Avoid_: controller (for this role)

**Kafka API**:
One request and response type of the Kafka protocol, named as the protocol names it (Produce, Fetch, JoinGroup) and identified by its api key.
_Avoid_: endpoint, command, RPC

**API version**:
The version of a Kafka API used with one broker: the highest version both the client and that broker support.

**Flexible version**:
The first version of a Kafka API that uses compact encodings and tagged fields (KIP-482).

## Topics and messages

**Topic**:
A named, append-only stream of messages, split into partitions.

**Partition**:
One ordered, append-only log within a topic. Ordering holds within a partition, not across a topic.

**Replica**:
A copy of a partition held by one broker. One replica is the partition leader; the others follow it.

**In-sync replica**:
A replica that is caught up with its partition leader closely enough to take over as leader.
_Avoid_: ISR (in prose)

**Partition leader**:
The replica, and so the broker, that accepts produce and fetch requests for one partition.
_Avoid_: partition owner

**Leader epoch**:
The number of a partition leader's current term. A request based on an older epoch is stale.

**Topic id**:
The broker-assigned unique id of a topic, used in place of its name by newer API versions.

**Message**:
One key, value, headers and timestamp entry that a user produces or consumes.
_Avoid_: event

**Record batch**:
The wire unit that carries messages to or from one partition (format v2, Kafka 0.11+). Older brokers use the legacy **message set** format (v0/v1) instead.

**Offset**:
A message's position within its partition.
_Avoid_: index, sequence

**High watermark**:
The offset just after the last message replicated to all in-sync replicas: the end of what consumers may read.

**Last stable offset**:
The offset before which no transaction is still open: the end of what a `read_committed` consumer may read.

**Tiered storage**:
Keeping a topic's older log segments in remote storage, such as an object store, while the leader's local disk keeps only the recent ones (KIP-405).

**Offset spec**:
Which offset an offset lookup returns for each partition, such as the earliest, the latest, the one with the largest timestamp, or a tiered storage boundary.

**Isolation level**:
Whether a consumer reads messages from open or aborted transactions (`read_uncommitted`) or only committed ones (`read_committed`).

## Producing

**Linger**:
The time the producer waits to collect more messages into a batch before sending it.

**Partitioner**:
The strategy that picks a partition for a message sent without one.

**Idempotent producer**:
A producer whose retries cannot write the same message twice to a partition.
_Avoid_: exactly-once producer (that also needs transactions)

**Producer id**:
The broker-assigned identity of an idempotent or transactional producer.
_Avoid_: PID (in prose)

**Producer epoch**:
A counter on the producer id that rises each time the identity is re-initialized.

**Epoch bump**:
Moving the producer to a higher producer epoch, which also restarts sequence numbers: by re-initializing the producer id (KIP-360), or, under transaction version 2, by the transaction coordinator as each transaction ends.

**Transaction version**:
The cluster-wide level of the transaction protocol, set by a finalized cluster feature. From version 2 (KIP-890), the transaction coordinator adds partitions and offsets to a transaction as they are written and bumps the producer epoch at the end of every transaction.

**Sequence number**:
A per-partition counter on produced batches that the broker uses to drop duplicate retries.

**Transactional id**:
The user-chosen, stable name that carries a transactional producer's identity across restarts.

**Transaction**:
A set of produced messages and consumer offset commits that become visible together or not at all.

## Consumer groups

**Consumer group**:
Consumers sharing a group id that split a topic's partitions so each partition belongs to one member.
_Avoid_: group (alone, where a share group or streams group could be meant)

**Member**:
One consumer within a consumer group, share group or streams group, identified by its member id.
_Avoid_: client, instance (when you mean the member rather than its group instance id)

**Static member**:
A member with a stable group instance id, so a restart within its session timeout does not trigger a rebalance.

**Group protocol**:
The membership protocol a consumer group uses: **classic** (JoinGroup/SyncGroup with client-side assigners) or **consumer** (KIP-848, broker-side assignment through heartbeats).
_Avoid_: old protocol, new protocol, next-gen protocol

**Group type**:
The kind of group a group coordinator tracks: classic, consumer, share or streams. A consumer group's group type follows its group protocol.

**Generation**:
The classic protocol's count of completed rebalances. Commits tagged with an older generation are rejected.
_Avoid_: epoch (for the classic protocol)

**Member epoch**:
The consumer protocol's per-member counter of assignment changes, its equivalent of the generation.
_Avoid_: generation (for the consumer protocol)

**Fenced**:
Rejected by the broker because a newer instance holds the same identity: a higher producer epoch, member epoch or generation.
_Avoid_: kicked, evicted, zombie

**Rebalance**:
A redistribution of partitions among a consumer group's members after membership or subscription changes.

**Eager rebalance**:
A rebalance in which every member revokes its whole assignment before partitions are redistributed.

**Cooperative rebalance**:
A rebalance in which members give up only the partitions that move to another member (KIP-429).
_Avoid_: incremental rebalance

**Assignment**:
The set of partitions a member currently owns.

**Assigner**:
A client-side strategy that computes assignments under the classic group protocol: range, round-robin, sticky or cooperative-sticky.
_Avoid_: assignor (except for the broker-side strategy of the consumer protocol, a share group or a streams group, which Kafka calls an assignor)

**Revoked**:
Given up cleanly by a member during a rebalance, after it had the chance to commit.

**Lost**:
Taken from a member without a clean revoke, because its session expired or it was fenced. Pending commits for lost partitions are abandoned.

**Assign mode**:
Consuming exact partitions chosen by the application, with no group membership and no rebalances.
_Avoid_: standalone consumer

**Committed offset**:
The offset a consumer group has stored for a partition: where consumption resumes after a restart or reassignment.

**Position**:
The offset of the next message the consumer will hand to the application for a partition.
_Avoid_: current offset

**Lag**:
How far a consumer's position trails the end of its partition.

**Seek**:
Moving a partition's position to a chosen offset.

**Pause**:
Stopping fetches for a partition without giving it up.
_Avoid_: suspend

**Fetch session**:
Per-broker incremental fetch state (KIP-227) that lets a fetch list only the partitions that changed.

## Share groups

**Share group**:
A group in which any member can receive any message from any partition, giving queue-like delivery (KIP-932). It has no partition assignment and no seek.
_Avoid_: queue (as a name), consumer group

**Acquired**:
Held by one share group member under a broker-side lock until the member acknowledges the message or the lock expires.
_Avoid_: leased, claimed

**Acknowledgement**:
A share group member's verdict on an acquired message: accept, release (make it available for redelivery), reject, or renew the lock.
_Avoid_: ack (in prose), commit

**Share session**:
A share group member's per-broker fetch state (KIP-932), the share group counterpart of a fetch session. Opening a new one releases the messages the previous one acquired.

## Streams groups

**Streams group**:
A group whose members run one Kafka Streams application and get their tasks from the group coordinator through heartbeats (KIP-1071).
_Avoid_: streams consumer group, application (as the group's name)

**Topology**:
The processing graph of a Kafka Streams application, split into subtopologies.

**Subtopology**:
A part of a topology that reads its own source topics and is split into tasks independently of the other subtopologies.
_Avoid_: sub-topology

**Task**:
One subtopology's processing of one partition of its source topics. A member runs it as an **active task**, keeps its state as a **standby task**, or restores that state as a **warm-up task** before taking it over.

**Topology description**:
The nodes and state stores of a streams group's topology, which a broker plugin can store and return when the group is described (KIP-1331).
