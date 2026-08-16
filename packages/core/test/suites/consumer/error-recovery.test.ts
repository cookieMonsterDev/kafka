import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import { KafkaNumberOfRetriesExceeded } from '../../../src/errors';
import type { EachMessagePayload } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

describe('consumer.errorRecovery', () => {
  let topicName: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    await createTopic({ topic: topicName });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  it('retries eachMessage and then delivers subsequent messages', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      retry: { retries: 5 },
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    let attempts = 0;
    const consumed: EachMessagePayload[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        if (event.message.offset === 0n && attempts < 2) {
          attempts += 1;
          throw new Error('transient handler failure');
        }
        consumed.push(event);
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 2 });
    expect(consumed.map((c) => c.message.offset)).toEqual([0n, 1n]);
    expect(attempts).toBe(2);
  });

  it('crashes when restartOnFailure returns false', async () => {
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      retry: { retries: 0, restartOnFailure: async () => false },
      logger: newLogger(),
    });
    await consumer.connect();
    await producer!.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });

    const crashed = new Promise<Error>((resolve) => {
      consumer!.on(consumer!.events.CRASH, (event) => {
        resolve((event.payload as { error: Error }).error);
      });
    });
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async () => {
        throw new KafkaNumberOfRetriesExceeded(new Error('fatal'), { retryCount: 0, retryTime: 0 });
      },
    });
    await join;
    await producer!.send({ acks: 1, topic: topicName, messages: [{ key: 'k', value: 'v' }] });
    await expect(crashed).resolves.toBeInstanceOf(Error);
  });
});
