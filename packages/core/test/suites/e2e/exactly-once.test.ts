import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index.js';
import { createProducer } from '../../../src/producer/index.js';
import type { EachMessagePayload } from '../../../src/consumer/types.js';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index.js';

describe('e2e.exactlyOnce', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('commits consumer offsets inside a producer transaction', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId: `txn-${secureRandom()}`,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const seed = createProducer({ cluster: createCluster(), logger: newLogger() });
    await seed.connect();
    await seed.send({ acks: -1, topic: topicName, messages: [{ key: 'in', value: 'input' }] });
    await seed.disconnect();

    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;
    await waitForMessages(consumed, { number: 1 });

    const outputTopic = `test-topic-${secureRandom()}`;
    await createTopic({ topic: outputTopic, partitions: 1 });
    const transaction = await producer.transaction();
    await transaction.send({ topic: outputTopic, messages: [{ key: 'out', value: 'output' }] });
    await transaction.sendOffsets({
      consumerGroupId: groupId,
      topics: [{ topic: topicName, partitions: [{ partition: 0, offset: consumed[0]!.message.offset + 1n }] }],
    });
    await transaction.commit();

    const sink = createConsumer({
      cluster: createCluster(),
      groupId: `sink-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    try {
      await sink.connect();
      await sink.subscribe({ topic: outputTopic, fromBeginning: true });
      const sinkMessages: EachMessagePayload[] = [];
      const sinkJoin = waitForConsumerToJoinGroup(sink);
      await sink.run({
        eachMessage: async (event) => {
          sinkMessages.push(event);
        },
      });
      await sinkJoin;
      await waitForMessages(sinkMessages, { number: 1 });
      expect(sinkMessages[0]?.message.value?.toString()).toBe('output');
    } finally {
      await sink.disconnect();
    }
  });
});
