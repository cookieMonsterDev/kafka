import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import type { Cluster } from '../../../src/cluster/index';
import { createConsumer } from '../../../src/consumer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import { createProducer } from '../../../src/producer/index';
import {
  advertisedAddress,
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_3_6,
  waitFor,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

// High enough that `refreshMetadataIfNecessary` never proactively refreshes mid-test, so the
// producer/consumer must recover the new leader from the error response itself (KIP-951),
// not from a Metadata RPC that happened to run first.
const METADATA_MAX_AGE = 5 * 60_000;

async function readLeader(cluster: Cluster, topic: string): Promise<number> {
  await cluster.refreshMetadata();
  const [partition] = cluster.findTopicPartitionMetadata(topic);
  if (!partition) throw new Error(`No cached partition metadata for ${topic}`);
  return partition.leader;
}

/**
 * Moves the partition-0 leader of `topic` to a different broker via a real reassignment +
 * preferred-leader election, then confirms the *old* leader itself has learned about the change
 * (querying any other broker would race the propagation this test depends on).
 */
async function moveLeadershipAway({
  admin,
  topic,
  oldLeader,
  nodeIds,
  nodeAddress,
}: {
  admin: ReturnType<typeof createAdmin>;
  topic: string;
  oldLeader: number;
  nodeIds: number[];
  nodeAddress: Map<number, { host: string; port: number }>;
}): Promise<number> {
  const newLeader = nodeIds.find((nodeId) => nodeId !== oldLeader);
  if (newLeader == null) {
    throw new Error('No alternate broker available to move leadership to');
  }

  await admin.alterPartitionReassignments({
    topics: [
      {
        topic,
        partitionAssignment: [{ partition: 0, replicas: [newLeader, ...nodeIds.filter((id) => id !== newLeader)] }],
      },
    ],
  });

  await waitFor(
    async () => {
      const { topics } = await admin.listPartitionReassignments({ topics: [{ topic, partitions: [0] }] });
      return topics.length === 0;
    },
    { maxWait: 15_000, timeoutMessage: `Timeout waiting for the reassignment of ${topic} to complete` },
  );

  await admin.electLeaders({ topicPartitions: [{ topic, partitions: [0] }] });

  const oldLeaderAddress = nodeAddress.get(oldLeader);
  if (!oldLeaderAddress) throw new Error(`No advertised address for broker ${oldLeader}`);

  const oldLeaderCluster = createCluster({}, [`${oldLeaderAddress.host}:${oldLeaderAddress.port}`]);
  try {
    await oldLeaderCluster.connect();
    await oldLeaderCluster.addTargetTopic(topic);
    await waitFor(async () => (await readLeader(oldLeaderCluster, topic)) === newLeader, {
      maxWait: 15_000,
      timeoutMessage: `Timeout waiting for broker ${oldLeader} to learn that ${newLeader} is the new leader`,
    });
  } finally {
    await oldLeaderCluster.disconnect();
  }

  return newLeader;
}

describe('cluster.leaderFailover', () => {
  let topicName: string;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let readerCluster: Cluster | undefined;
  let nodeIds: number[];
  let nodeAddress: Map<number, { host: string; port: number }>;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1, replicas: 3 });

    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const { brokers } = await admin.describeCluster();
    nodeIds = brokers.map((broker) => broker.nodeId);
    nodeAddress = new Map(brokers.map((broker) => [broker.nodeId, advertisedAddress(broker.host, broker.port)]));

    readerCluster = createCluster();
    await readerCluster.connect();
    await readerCluster.addTargetTopic(topicName);
  });

  afterEach(async () => {
    await admin?.disconnect();
    await readerCluster?.disconnect();
  });

  testIfKafkaAtLeast_3_6(
    'producer recovers a real leader change from the Produce response, without a Metadata RPC',
    async () => {
      const producerCluster = createCluster({ metadataMaxAge: METADATA_MAX_AGE });
      const producer = createProducer({ cluster: producerCluster, logger: newLogger() });

      try {
        await producer.connect();
        // Prime the producer's cache with the leader that is about to be moved away.
        await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 1 }) });

        const oldLeader = await readLeader(readerCluster!, topicName);
        await moveLeadershipAway({ admin: admin!, topic: topicName, oldLeader, nodeIds, nodeAddress });

        let metadataRequests = 0;
        const removeListener = producer.on(producer.events.REQUEST, (event) => {
          if ((event.payload as { apiName: string }).apiName === 'Metadata') metadataRequests += 1;
        });

        await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
        removeListener();

        expect(metadataRequests).toBe(0);
      } finally {
        await producer.disconnect();
        await producerCluster.disconnect();
      }
    },
  );

  testIfKafkaAtLeast_3_6(
    'consumer recovers a real leader change from the Fetch response, without a Metadata RPC',
    async () => {
      const producer = createProducer({ cluster: createCluster(), logger: newLogger() });
      const consumerCluster = createCluster({ metadataMaxAge: METADATA_MAX_AGE });
      const consumer = createConsumer({
        cluster: consumerCluster,
        groupId: `group-${secureRandom()}`,
        maxWaitTimeInMs: 100,
        logger: newLogger(),
      });

      try {
        await producer.connect();
        await consumer.connect();
        await consumer.subscribe({ topic: topicName, fromBeginning: true });

        const consumed: EachMessagePayload[] = [];
        const join = waitForConsumerToJoinGroup(consumer);
        await consumer.run({
          eachMessage: async (event) => {
            consumed.push(event);
          },
        });
        await join;

        // Prime the consumer's cache with the leader that is about to be moved away.
        await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 1 }) });
        await waitForMessages(consumed, { number: 1 });

        const oldLeader = await readLeader(readerCluster!, topicName);
        await moveLeadershipAway({ admin: admin!, topic: topicName, oldLeader, nodeIds, nodeAddress });

        let metadataRequests = 0;
        const removeListener = consumer.on(consumer.events.REQUEST, (event) => {
          if ((event.payload as { apiName: string }).apiName === 'Metadata') metadataRequests += 1;
        });

        await producer.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
        await waitForMessages(consumed, { number: 3 });
        removeListener();

        expect(metadataRequests).toBe(0);
      } finally {
        await consumer.disconnect();
        await producer.disconnect();
        await consumerCluster.disconnect();
      }
    },
  );
});
