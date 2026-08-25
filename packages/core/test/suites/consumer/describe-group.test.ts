import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { createCluster, createTopic, newLogger, secureRandom, waitForConsumerToJoinGroup } from '../../helpers/index';

describe('consumer.describeGroup', () => {
  let topicName: string;
  let groupId: string;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `group-${secureRandom()}`;
    await createTopic({ topic: topicName });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
  });

  afterEach(async () => {
    await consumer?.disconnect();
  });

  it('describes the group after the member has joined', async () => {
    await consumer!.connect();
    await consumer!.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer!);
    await consumer!.run({ eachMessage: async () => undefined });
    await join;

    const described = await consumer!.describeGroup();
    expect(described.groupId).toBe(groupId);
    expect(described.members).toHaveLength(1);
    expect(described.members[0]?.memberId).toEqual(expect.any(String));
    expect(described.protocolType).toBe('consumer');
    expect(described.state).toEqual(expect.any(String));
  });
});
