import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createAssignReplicasToDirsApi } from './assign-replicas-to-dirs';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const directoryA = Buffer.alloc(16, 1);
const directoryB = Buffer.alloc(16, 2);
const eventsTopicId = Buffer.alloc(16, 9);
const logsTopicId = Buffer.alloc(16, 8);

function makeApi(cluster: ReturnType<typeof fakeRunCluster>) {
  return createAssignReplicasToDirsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

describe('admin/assign-replicas-to-dirs', () => {
  it('groups replicas by directory and topic then targets the controller', async () => {
    const broker = withSend({ assignReplicasToDirs: vi.fn().mockResolvedValue({ errorCode: 0, directories: [] }) });
    const addMultipleTargetTopics = vi.fn().mockResolvedValue(undefined);
    const cluster = fakeRunCluster(
      { controller: broker },
      {
        addMultipleTargetTopics,
        findTopicId: vi.fn((topic: string) => (topic === 'events' ? eventsTopicId : logsTopicId)),
      },
    );
    const api = makeApi(cluster);

    await expect(
      api.assignReplicasToDirs({
        brokerId: 3,
        replicas: [
          { topic: 'events', partition: 0, directoryId: directoryA },
          { topic: 'events', partition: 1, directoryId: directoryA },
          { topic: 'logs', partition: 0, directoryId: directoryB },
        ],
      }),
    ).resolves.toBeUndefined();

    expect(addMultipleTargetTopics).toHaveBeenCalledWith(['events', 'logs']);
    expect(cluster.refreshMetadata).toHaveBeenCalled();
    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
    expect(broker.assignReplicasToDirs).toHaveBeenCalledWith({
      brokerId: 3,
      brokerEpoch: -1n,
      directories: [
        {
          id: directoryA,
          topics: [{ topicId: eventsTopicId, partitions: [{ partitionIndex: 0 }, { partitionIndex: 1 }] }],
        },
        {
          id: directoryB,
          topics: [{ topicId: logsTopicId, partitions: [{ partitionIndex: 0 }] }],
        },
      ],
    });
  });

  it('rejects invalid broker ids and replica lists before contacting the cluster', async () => {
    const cluster = fakeRunCluster({ controller: withSend({}) }, { addMultipleTargetTopics: vi.fn() });
    const api = makeApi(cluster);

    await expect(api.assignReplicasToDirs({ brokerId: -1, replicas: [] })).rejects.toThrow(KafkaNonRetriableError);
    await expect(
      api.assignReplicasToDirs({
        brokerId: 1,
        replicas: [{ topic: 'events', partition: 0, directoryId: Buffer.from([1, 2, 3]) }],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
    expect(cluster.run).not.toHaveBeenCalled();
  });

  it('throws when a topic has no topic id', async () => {
    const broker = withSend({ assignReplicasToDirs: vi.fn() });
    const cluster = fakeRunCluster(
      { controller: broker },
      {
        addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
        findTopicId: vi.fn().mockReturnValue(undefined),
      },
    );
    const api = makeApi(cluster);

    await expect(
      api.assignReplicasToDirs({
        brokerId: 1,
        replicas: [{ topic: 'events', partition: 0, directoryId: directoryA }],
      }),
    ).rejects.toThrow(/No topic id for topic events/);
    // The missing topic id rejects without a retry and before any request reaches the controller.
    expect(cluster.recover).not.toHaveBeenCalled();
    expect(broker.assignReplicasToDirs).not.toHaveBeenCalled();
  });
});
