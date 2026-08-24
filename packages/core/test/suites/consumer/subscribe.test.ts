import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createCluster, createTopic, newLogger, secureRandom, waitForConsumerToJoinGroup } from '../../helpers/index';

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
