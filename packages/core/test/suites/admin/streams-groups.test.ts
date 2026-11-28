import { randomUUID } from 'node:crypto';
import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { STREAMS_TOPOLOGY_DESCRIPTION_STATUSES } from '../../../src/protocol/enums/streams-topology-description-statuses';
import {
  createCluster,
  createTopic,
  newLogger,
  secureRandom,
  testIfKafkaAtLeast_4_4,
  testIfKafkaAtMost_4_1,
} from '../../helpers/index';
import { joinStreamsGroup } from '../../helpers/streams-group-heartbeat';

describe('admin.streamsGroups', () => {
  let groupId: string;
  let admin: ReturnType<typeof createAdmin> | undefined;

  beforeEach(async () => {
    groupId = `test-streams-group-${secureRandom()}`;
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();
  });

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_4_4('describes a streams group with its topology and member', async () => {
    const topic = `test-topic-${secureRandom()}`;
    await createTopic({ topic, partitions: 2 });
    const cluster = createCluster();
    const memberId = randomUUID();
    const processId = randomUUID();
    try {
      await cluster.connect();
      const joined = await joinStreamsGroup({ cluster, groupId, memberId, processId, sourceTopic: topic });
      expect(joined).toMatchObject({ errorCode: 0, memberId });
    } finally {
      await cluster.disconnect();
    }

    const { groups } = await admin!.describeStreamsGroups([groupId], { includeTopologyDescription: true });

    expect(groups).toHaveLength(1);
    const [group] = groups;
    expect(group).toMatchObject({
      errorCode: 0,
      groupId,
      groupState: expect.any(String),
      topology: { epoch: 0, subtopologies: [expect.objectContaining({ subtopologyId: '0', sourceTopics: [topic] })] },
      assignorName: expect.any(String),
      // No topology description plugin is configured on the test brokers.
      topologyDescription: null,
      topologyDescriptionStatus: STREAMS_TOPOLOGY_DESCRIPTION_STATUSES.NOT_STORED,
    });
    expect(group?.members).toEqual([
      expect.objectContaining({ memberId, processId, topologyEpoch: 0, userEndpoint: null, isClassic: false }),
    ]);

    const listed = await admin!.listGroups({ types: ['streams'] });
    expect(listed.groups).toContainEqual(
      expect.objectContaining({ groupId, protocolType: 'streams', groupType: 'streams' }),
    );
  });

  testIfKafkaAtLeast_4_4('rejects an unknown streams group with GROUP_ID_NOT_FOUND', async () => {
    await expect(admin!.describeStreamsGroups([groupId])).rejects.toMatchObject({
      cause: { type: 'GROUP_ID_NOT_FOUND' },
    });
    await expect(admin!.describeStreamsGroups([groupId], { includeTopologyDescription: true })).rejects.toMatchObject({
      cause: { type: 'GROUP_ID_NOT_FOUND' },
    });
  });

  testIfKafkaAtMost_4_1('rejects describeStreamsGroups on a broker without StreamsGroupDescribe', async () => {
    await expect(admin!.describeStreamsGroups([groupId])).rejects.toMatchObject({
      cause: { name: 'KafkaServerDoesNotSupportApiKey', apiName: 'StreamsGroupDescribe' },
    });
  });
});
