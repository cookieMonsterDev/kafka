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

describe('consumer.pauseResume', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 2 });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 50,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('returns no paused partitions before run', () => {
    expect(consumer!.paused()).toEqual([]);
  });

  it('pauses and resumes a partition', async () => {
    await consumer!.connect();
    await producer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [
        { key: 'a', value: 'a', partition: 0 },
        { key: 'b', value: 'b', partition: 1 },
      ],
    });
    await waitForMessages(consumed, { number: 2 });

    consumer!.pause([{ topic: topicName, partitions: [0] }]);
    expect(consumer!.paused()).toEqual(expect.arrayContaining([{ topic: topicName, partitions: [0] }]));

    const before = consumed.length;
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [
        { key: 'c', value: 'c', partition: 0 },
        { key: 'd', value: 'd', partition: 1 },
      ],
    });
    await waitForMessages(consumed, { number: before + 1 });
    expect(consumed.slice(before).every((e) => e.partition === 1)).toBe(true);

    consumer!.resume([{ topic: topicName, partitions: [0] }]);
    await waitForMessages(consumed, { number: before + 2 });
  });
});
