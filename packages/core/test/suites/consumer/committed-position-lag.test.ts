import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import { KafkaNonRetriableError } from '../../../src/errors';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createModPartitioner,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.committed/position/currentLag', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 1 });
    producer = createProducer({
      cluster: createCluster(),
      createPartitioner: createModPartitioner,
      logger: newLogger(),
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('rejects position and currentLag before run', () => {
    expect(() => consumer!.position({ topic: topicName, partition: 0 })).toThrow(KafkaNonRetriableError);
    expect(() => consumer!.currentLag({ topic: topicName, partition: 0 })).toThrow(KafkaNonRetriableError);
  });

  it('reads back -1n for a partition with no committed offset', async () => {
    await consumer!.connect();
    const committed = await consumer!.committed([{ topic: topicName, partition: 0 }]);
    expect(committed).toEqual([{ topic: topicName, partition: 0, offset: -1n, metadata: null }]);
  });

  it('reads back a committed offset without a running consumer group', async () => {
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
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 3 }) });
    await waitForMessages(consumed, { number: 3 });
    await consumer!.commitOffsets([{ topic: topicName, partition: 0, offset: 3n }]);
    await consumer!.stop();

    const committed = await consumer!.committed([{ topic: topicName, partition: 0 }]);
    expect(committed).toEqual([{ topic: topicName, partition: 0, offset: 3n, metadata: null }]);
  });

  it('tracks position and currentLag as messages are consumed, and returns null for an unassigned partition', async () => {
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

    // Topic has one partition; partition 1 is never assigned to this consumer.
    expect(consumer!.position({ topic: topicName, partition: 1 })).toBeNull();
    expect(consumer!.currentLag({ topic: topicName, partition: 1 })).toBeNull();

    // No committed offset and `fromBeginning: true`, so the resolved starting position is 0.
    expect(consumer!.position({ topic: topicName, partition: 0 })).toBe(0n);

    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
    await waitForMessages(consumed, { number: 5 });

    expect(consumer!.position({ topic: topicName, partition: 0 })).toBe(5n);
    expect(consumer!.currentLag({ topic: topicName, partition: 0 })).toBe(0n);
  });
});
