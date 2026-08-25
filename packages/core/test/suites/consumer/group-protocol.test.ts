import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { InstrumentationEventEmitter } from '../../../src/instrumentation/emitter';
import { createProducer } from '../../../src/producer/index';
import type { EachMessagePayload, MemberAssignment } from '../../../src/consumer/types';
import {
  createCluster,
  createTopic,
  generateMessages,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtLeast_4_1,
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
    expect([...firstPartitions].some((partition) => secondPartitions.has(partition))).toBe(false);
    expect(new Set([...firstPartitions, ...secondPartitions]).size).toBe(2);
  });

  testIfKafkaAtLeast_4_1(
    'a RegExp subscription is matched server-side via subscribedTopicRegex (KIP-848 SubscriptionPattern)',
    async () => {
      // `topicName` already exists (created in beforeEach). The broker matches
      // `subscribedTopicRegex` against topics it currently knows about, it does not discover
      // topics created after the member subscribes - so the unmatched topic is created up front.
      // Gated to 4.1+ because `subscribedTopicRegex` is ConsumerGroupHeartbeat v1.
      const unmatchedTopic = `unmatched-${secureRandom()}`;
      await createTopic({ topic: unmatchedTopic, partitions: 1 });

      first = createConsumer({
        cluster: createCluster(),
        groupId,
        groupProtocol: 'consumer',
        maxWaitTimeInMs: 100,
        rebalanceTimeout: 15_000,
        logger: newLogger(),
      });

      await first.connect();
      await first.subscribe({ topics: [new RegExp(`^${topicName}$`)], fromBeginning: true });

      const consumed: EachMessagePayload[] = [];
      const joined = waitForAssignedPartitions(first, { label: 'regex-subscribe' });
      await first.run({
        eachMessage: async (event) => {
          consumed.push(event);
        },
      });
      await joined;

      await producer!.connect();
      await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 5 }) });
      await producer!.send({ acks: 1, topic: unmatchedTopic, messages: generateMessages({ number: 5 }) });

      await waitForMessages(consumed, { number: 5 });

      expect(consumed.every((event) => event.topic === topicName)).toBe(true);
    },
  );

  testIfKafkaAtLeast_4_0('sends groupRemoteAssignor uniform on ConsumerGroupHeartbeat', async () => {
    const instrumentationEmitter = new InstrumentationEventEmitter();
    first = createConsumer({
      cluster: createCluster({ instrumentationEmitter }),
      groupId,
      groupProtocol: 'consumer',
      groupRemoteAssignor: 'uniform',
      maxWaitTimeInMs: 100,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
      instrumentationEmitter,
    });

    const heartbeats: unknown[] = [];
    first.on(first.events.REQUEST, (event) => {
      const payload = event.payload as { apiName: string };
      if (payload.apiName === 'ConsumerGroupHeartbeat') heartbeats.push(payload);
    });

    await first.connect();
    await first.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: EachMessagePayload[] = [];
    const joined = waitForAssignedPartitions(first, { label: 'uniform-assignor' });
    await first.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await joined;

    expect(heartbeats.length).toBeGreaterThan(0);

    await producer!.connect();
    await producer!.send({ acks: 1, topic: topicName, messages: generateMessages({ number: 2 }) });
    await waitForMessages(consumed, { number: 2 });
  });
});
