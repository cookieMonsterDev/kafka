import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload, MemberAssignment } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_4_0,
  waitForMessages,
} from '../../helpers/index';

function waitForAssignedPartitions(
  consumer: ReturnType<typeof createConsumer>,
  { maxWait = 20_000, label = '' }: { maxWait?: number; label?: string } = {},
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error(`Timeout ${label}`.trim()));
      void consumer.disconnect();
    }, maxWait);
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      const assignment = (event.payload as { memberAssignment?: MemberAssignment }).memberAssignment ?? {};
      if (Object.values(assignment).flat().length > 0) {
        clearTimeout(timeoutId);
        resolve(event);
      }
    });
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId);
      reject((event.payload as { error: Error }).error);
      void consumer.disconnect();
    });
  });
}

describe('consumer.groupProtocol', () => {
  let topicName: string;
  let groupId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let first: ReturnType<typeof createConsumer> | undefined;
  let second: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 2 });
    producer = createProducer({ cluster: createCluster(), logger: newLogger() });
  });

  afterEach(async () => {
    await first?.disconnect();
    await second?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_4_0('two consumers with groupProtocol consumer share partitions and consume', async () => {
    first = createConsumer({
      cluster: createCluster(),
      groupId,
      groupProtocol: 'consumer',
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });
    second = createConsumer({
      cluster: createCluster(),
      groupId,
      groupProtocol: 'consumer',
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });

    await first.connect();
    await first.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const firstPartitions = new Set<number>();
    const firstJoin = waitForAssignedPartitions(first, { label: 'first' });
    await first.run({
      eachMessage: async (event) => {
        firstPartitions.add(event.partition);
        consumed.push(event);
      },
    });
    await firstJoin;

    const firstRebalance = waitForAssignedPartitions(first, { label: 'first-rebalance', maxWait: 20_000 });
    await second.connect();
    await second.subscribe({ topic: topicName, fromBeginning: true });
    const secondPartitions = new Set<number>();
    const secondJoin = waitForAssignedPartitions(second, { label: 'second', maxWait: 20_000 });
    await second.run({
      eachMessage: async (event) => {
        secondPartitions.add(event.partition);
        consumed.push(event);
      },
    });
    await Promise.all([firstRebalance, secondJoin]);

    await producer!.connect();
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: generateMessages({ number: 20 }),
    });

    await waitForMessages(consumed, { number: 20 });

    expect(firstPartitions.size).toBeGreaterThan(0);
    expect(secondPartitions.size).toBeGreaterThan(0);
    expect(new Set([...firstPartitions, ...secondPartitions]).size).toBeGreaterThan(0);
  });
});
