import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createConsumer } from '../../../src/consumer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_2_4,
  waitForConsumerToJoinGroup,
} from '../../helpers/index';

describe('admin.batch-a removeMembersFromConsumerGroup', () => {
  let topicName: string;
  let groupId: string;
  let admin: ReturnType<typeof createAdmin> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    groupId = `test-group-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_2_4('removes a group member through LeaveGroup', async () => {
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });

    await admin.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({ eachMessage: async () => undefined });
    await join;

    const described = await admin.describeGroups([groupId]);
    const memberId = described.groups[0]?.members[0]?.memberId;
    expect(memberId).toEqual(expect.any(String));

    const removed = await admin.removeMembersFromConsumerGroup({
      groupId,
      members: [{ memberId: memberId! }],
    });
    expect(removed.members[0]?.errorCode ?? 0).toBe(0);

    await consumer.disconnect();
    consumer = undefined;
  });
});
