import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_2_4,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.staticMembership', () => {
  let topicName: string;
  let groupId: string;
  let groupInstanceId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    groupInstanceId = `instance-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_2_4('rejoins with the same groupInstanceId and resumes consuming', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      groupInstanceId,
      maxWaitTimeInMs: 100,
      sessionTimeout: 15_000,
      rebalanceTimeout: 20_000,
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const described = await consumer.describeGroup();
    expect(described.members).toHaveLength(1);
    expect(described.groupId).toBe(groupId);

    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 2 });
    await consumer.disconnect();
    consumer = undefined;

    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      groupInstanceId,
      maxWaitTimeInMs: 100,
      sessionTimeout: 15_000,
      rebalanceTimeout: 20_000,
      logger: newLogger(),
    });
    await consumer.connect();
    await consumer.subscribe({ topic: topicName });
    const rejoin = waitForConsumerToJoinGroup(consumer, { maxWait: 20_000 });
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await rejoin;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 4 });
    expect(consumed.map((event) => event.message.offset)).toEqual([0n, 1n, 2n, 3n]);
  });
});
