import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.subscribe', () => {
  let topicName: string;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await consumer.connect();
  });

  afterEach(async () => {
    await consumer?.disconnect();
  });

  it('subscribes to a topic by name', async () => {
    await expect(consumer!.subscribe({ topic: topicName, fromBeginning: true })).resolves.toBeUndefined();
  });

  it('subscribes to multiple topics', async () => {
    const other = `test-topic-${secureRandom()}`;
    await createTopic({ topic: other });
    await expect(consumer!.subscribe({ topics: [topicName, other], fromBeginning: true })).resolves.toBeUndefined();
  });

  it('subscribes with a regex', async () => {
    await expect(
      consumer!.subscribe({ topic: new RegExp(`^${topicName}$`), fromBeginning: true }),
    ).resolves.toBeUndefined();
  });

  it('consumes from topics that match a regex subscription', async () => {
    const producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    const prefix = `rgx-${secureRandom().slice(0, 8)}`;
    const matching = `${prefix}-a-${secureRandom()}`;
    const otherMatching = `${prefix}-b-${secureRandom()}`;
    await createTopic({ topic: matching });
    await createTopic({ topic: otherMatching });

    try {
      await producer.connect();
      await consumer!.subscribe({ topic: new RegExp(`^${prefix}-`), fromBeginning: true });
      const consumed: string[] = [];
      const join = waitForConsumerToJoinGroup(consumer!);
      await consumer!.run({
        eachMessage: async (event) => {
          consumed.push(event.topic);
        },
      });
      await join;
      await producer.send({ acks: 1, topic: matching, messages: generateMessages({ number: 1 }) });
      await producer.send({ acks: 1, topic: otherMatching, messages: generateMessages({ number: 1 }) });
      await waitForMessages(consumed, { number: 2 });
      expect(new Set(consumed)).toEqual(new Set([matching, otherMatching]));
    } finally {
      await producer.disconnect();
    }
  });

  it('rejects a second subscribe while the consumer is running', async () => {
    const other = `test-topic-${secureRandom()}`;
    await createTopic({ topic: other });
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({ eachMessage: async () => undefined });
    await join;
    await expect(consumer!.subscribe({ topic: other })).rejects.toThrow(
      'Cannot subscribe to topic while consumer is running',
    );
  });
});
