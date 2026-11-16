import { afterEach, beforeEach, describe, expect } from 'vitest';
import type { CommittedOffsetsByGroup } from '../../../src/cluster/index';
import { cooperativeSticky } from '../../../src/consumer/assigners/index';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitFor,
  waitForConsumerToJoinGroup,
  waitForMessages,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_2_4,
} from '../../helpers/index';

describe('consumer.transactions', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;
  let secondConsumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await secondConsumer?.disconnect();
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_0_11('does not deliver aborted transactional messages under read-committed', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId: `txn-${secureRandom()}`,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
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

    const aborted = await producer.transaction();
    await aborted.send({ topic: topicName, messages: [{ key: 'a', value: 'aborted' }] });
    await aborted.abort();

    const committed = await producer.transaction();
    await committed.send({ topic: topicName, messages: [{ key: 'c', value: 'committed' }] });
    await committed.commit();

    await waitForMessages(consumed, { number: 1 });
    expect(consumed.map((c) => c.message.value?.toString())).toEqual(['committed']);
  });

  testIfKafkaAtLeast_2_4(
    'auto-commits a processed offset whose transaction aborted, on a partition retained across a rebalance',
    async () => {
      // Shared like the record of one Kafka client, so the producer's offset marks reach the consumer.
      const offsets: CommittedOffsetsByGroup = new Map();
      const groupId = `group-${secureRandom()}`;
      const inputTopic = `test-topic-${secureRandom()}`;
      const outputTopic = `test-topic-${secureRandom()}`;
      await createTopic({ topic: inputTopic, partitions: 2 });
      await createTopic({ topic: outputTopic, partitions: 1 });

      const seed = createProducer({ cluster: createCluster(), logger: newLogger() });
      await seed.connect();
      await seed.send({
        acks: -1,
        topic: inputTopic,
        messages: [
          { partition: 0, value: 'zero' },
          { partition: 1, value: 'one' },
        ],
      });
      await seed.disconnect();

      producer = createProducer({
        cluster: createCluster({ offsets }),
        logger: newLogger(),
        idempotent: true,
        transactionalId: `txn-${secureRandom()}`,
      });
      const createGroupMember = () =>
        createConsumer({
          cluster: createCluster({ offsets }),
          groupId,
          partitionAssigners: [cooperativeSticky],
          maxWaitTimeInMs: 100,
          sessionTimeout: 10_000,
          rebalanceTimeout: 15_000,
          logger: newLogger(),
        });
      consumer = createGroupMember();
      secondConsumer = createGroupMember();
      await producer.connect();
      await consumer.connect();
      await consumer.subscribe({ topic: inputTopic, fromBeginning: true });

      const consumed: EachMessagePayload[] = [];
      const join = waitForConsumerToJoinGroup(consumer, { label: 'first' });
      await consumer.run({
        eachMessage: async (event) => {
          const { topic, partition, message } = event;
          const transaction = await producer!.transaction();
          try {
            await transaction.send({ topic: outputTopic, messages: [{ value: message.value }] });
            await transaction.sendOffsets({
              consumerGroupId: groupId,
              topics: [{ topic, partitions: [{ partition, offset: message.offset + 1n }] }],
            });
          } finally {
            // Also when a step fails, so the redelivered message can begin a new transaction.
            await transaction.abort();
          }
          consumed.push(event);
        },
      });
      await join;
      await waitForMessages(consumed, { number: 2 });

      const rejoin = waitForConsumerToJoinGroup(consumer, { label: 'first-rejoin', maxWait: 20_000 });
      await secondConsumer.connect();
      await secondConsumer.subscribe({ topic: inputTopic, fromBeginning: true });
      const secondJoin = waitForConsumerToJoinGroup(secondConsumer, { label: 'second', maxWait: 20_000 });
      await secondConsumer.run({ eachMessage: async () => undefined });
      await Promise.all([rejoin, secondJoin]);

      const retained = [0, 1].filter((partition) => consumer!.position({ topic: inputTopic, partition }) !== null);
      expect(retained).toHaveLength(1);
      const [partition] = retained as [number];

      // The retained partition keeps its committed offset without an OffsetFetch. The aborted
      // transaction must not have marked its offset committed there, or auto-commit skips it.
      await waitFor(
        async () => {
          const [committed] = await consumer!.committed([{ topic: inputTopic, partition }]);
          return committed?.offset === 1n;
        },
        { maxWait: 10_000, timeoutMessage: 'The retained partition was never committed' },
      );
    },
  );
});
