import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { cooperativeSticky } from '../../../src/consumer/assigners/index';
import { MemberAssignment } from '../../../src/consumer/assigner-protocol';
import { createConsumer } from '../../../src/consumer/index';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_2_4,
  waitForConsumerToJoinGroup,
} from '../../helpers/index';

describe('consumer.rebalance', () => {
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

  it('rebalances when a second member joins', async () => {
    first = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });
    second = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });

    await first.connect();
    await first.subscribe({ topic: topicName, fromBeginning: true });
    const firstJoin = waitForConsumerToJoinGroup(first, { label: 'first' });
    await first.run({ eachMessage: async () => undefined });
    await firstJoin;

    const rejoin = waitForConsumerToJoinGroup(first, { label: 'first-rejoin', maxWait: 20_000 });
    await second.connect();
    await second.subscribe({ topic: topicName, fromBeginning: true });
    const secondJoin = waitForConsumerToJoinGroup(second, { label: 'second', maxWait: 20_000 });
    await second.run({ eachMessage: async () => undefined });
    await Promise.all([rejoin, secondJoin]);

    const [a, b] = await Promise.all([first.describeGroup(), second.describeGroup()]);
    expect(a.members.length).toBe(2);
    expect(b.members.length).toBe(2);
  });

  testIfKafkaAtLeast_2_4('settles a cooperative rebalance before reporting the group join', async () => {
    first = createConsumer({
      cluster: createCluster(),
      groupId,
      partitionAssigners: [cooperativeSticky],
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });
    second = createConsumer({
      cluster: createCluster(),
      groupId,
      partitionAssigners: [cooperativeSticky],
      maxWaitTimeInMs: 100,
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      logger: newLogger(),
    });

    await first.connect();
    await first.subscribe({ topic: topicName, fromBeginning: true });
    const firstJoin = waitForConsumerToJoinGroup(first, { label: 'cooperative-first' });
    await first.run({ eachMessage: async () => undefined });
    await firstJoin;

    const firstRejoin = waitForConsumerToJoinGroup(first, {
      label: 'cooperative-first-rejoin',
      maxWait: 20_000,
    });
    await second.connect();
    await second.subscribe({ topic: topicName, fromBeginning: true });
    const secondJoin = waitForConsumerToJoinGroup(second, {
      label: 'cooperative-second',
      maxWait: 20_000,
    });
    await second.run({ eachMessage: async () => undefined });
    await Promise.all([firstRejoin, secondJoin]);

    const group = await first.describeGroup();
    const assignmentSizes = group.members.map(
      ({ memberAssignment }) =>
        Object.values(MemberAssignment.decode(memberAssignment)?.assignment ?? {}).flat().length,
    );

    expect(group.protocol).toBe('cooperative-sticky');
    expect(assignmentSizes).toEqual([1, 1]);
  });
});
