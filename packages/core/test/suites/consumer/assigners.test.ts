import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { range, sticky } from '../../../src/consumer/assigners/index';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload, MemberAssignment } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  waitForConsumerToJoinGroup,
  waitForMessages,
} from '../../helpers/index';

function assignedPartitions(
  consumer: ReturnType<typeof createConsumer>,
  { maxWait = 15_000 }: { maxWait?: number } = {},
): Promise<number[]> {
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      reject(new Error('Timeout waiting for a non-empty assignment'));
    }, maxWait);
    consumer.on(consumer.events.GROUP_JOIN, (event) => {
      const assignment = (event.payload as { memberAssignment?: MemberAssignment }).memberAssignment ?? {};
      const partitions = Object.values(assignment).flat();
      if (partitions.length > 0) {
        clearTimeout(timeoutId);
        resolve(partitions);
      }
    });
    consumer.on(consumer.events.CRASH, (event) => {
      clearTimeout(timeoutId);
      reject((event.payload as { error: Error }).error);
    });
  });
}

describe('consumer.assigners', () => {
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

  it.each([
    { name: 'range', partitionAssigners: [range] },
    { name: 'sticky', partitionAssigners: [sticky] },
  ])('splits two partitions across two $name members', async ({ partitionAssigners }) => {
    first = createConsumer({
      cluster: createCluster(),
      groupId,
      partitionAssigners,
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });
    second = createConsumer({
      cluster: createCluster(),
      groupId,
      partitionAssigners,
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });

    await first.connect();
    await first.subscribe({ topic: topicName, fromBeginning: true });
    const firstJoin = waitForConsumerToJoinGroup(first, { label: 'first' });
    const consumed: EachMessagePayload[] = [];
    await first.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await firstJoin;

    await second.connect();
    await second.subscribe({ topic: topicName, fromBeginning: true });
    const firstRejoin = assignedPartitions(first);
    const secondJoin = assignedPartitions(second);
    await second.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    const [firstPartitions, secondPartitions] = await Promise.all([firstRejoin, secondJoin]);

    expect(firstPartitions).toHaveLength(1);
    expect(secondPartitions).toHaveLength(1);
    expect(new Set([...firstPartitions, ...secondPartitions])).toEqual(new Set([0, 1]));

    await producer!.connect();
    await producer!.send({
      acks: 1,
      topic: topicName,
      messages: [
        { key: 'a', value: 'a', partition: 0 },
        { key: 'b', value: 'b', partition: 1 },
      ],
    });
    await waitForMessages(consumed, { number: 2 });
    expect(new Set(consumed.map((event) => event.partition))).toEqual(new Set([0, 1]));
  });
});
