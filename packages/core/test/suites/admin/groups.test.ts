import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { createConsumer } from '../../../src/consumer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_1_1,
  testIfKafkaAtLeast_2_4,
  waitFor,
  waitForConsumerToJoinGroup,
} from '../../helpers/index';

describe('admin.groups', () => {
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

  it('lists and describes consumer groups', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
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

    const listed = await admin.listGroups();
    expect(listed.groups.map((g) => g.groupId)).toEqual(expect.arrayContaining([groupId]));

    const described = await admin.describeGroups([groupId]);
    expect(described.groups[0]?.groupId).toBe(groupId);
    expect(described.groups[0]?.members.length).toBeGreaterThan(0);
  });

  testIfKafkaAtLeast_1_1('deletes consumer groups', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
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
    await consumer.disconnect();
    consumer = undefined;

    await waitFor(async () => {
      const described = await admin!.describeGroups([groupId]);
      const state = described.groups[0]?.state;
      return state === 'Empty' || state === 'Dead' ? state : false;
    });

    const deleted = await admin.deleteGroups([groupId]);
    expect(deleted[0]?.groupId).toBe(groupId);
    expect(deleted[0]?.errorCode ?? 0).toBe(0);
  });

  testIfKafkaAtLeast_2_4('deletes committed offsets for an empty group', async () => {
    const cluster = createCluster();
    admin = createAdmin({ cluster, logger: newLogger() });
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
    await consumer.disconnect();
    consumer = undefined;

    await waitFor(async () => {
      const described = await admin!.describeGroups([groupId]);
      const state = described.groups[0]?.state;
      return state === 'Empty' || state === 'Dead' ? state : false;
    });

    const deleted = await admin.deleteGroupOffsets({
      groupId,
      topics: [{ topic: topicName, partitions: [0] }],
    });
    expect(deleted.topics).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: topicName,
          partitions: expect.arrayContaining([expect.objectContaining({ partitionIndex: 0, errorCode: 0 })]),
        }),
      ]),
    );
  });
});
