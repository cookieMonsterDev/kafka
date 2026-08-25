import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createAssignReplicasToDirsApi } from './assign-replicas-to-dirs';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const directoryA = Buffer.alloc(16, 1);
const directoryB = Buffer.alloc(16, 2);
const eventsTopicId = Buffer.alloc(16, 9);
const logsTopicId = Buffer.alloc(16, 8);

describe('admin/assign-replicas-to-dirs', () => {
  it('groups replicas by directory and topic then targets the controller', async () => {
    const broker = { assignReplicasToDirs: vi.fn().mockResolvedValue({ errorCode: 0, directories: [] }) };
    const addMultipleTargetTopics = vi.fn().mockResolvedValue(undefined);
    const cluster = {
      addMultipleTargetTopics,
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findTopicId: vi.fn((topic: string) => (topic === 'events' ? eventsTopicId : logsTopicId)),
      findControllerBroker: vi.fn().mockResolvedValue(broker),
    } as unknown as Cluster;
    const api = createAssignReplicasToDirsApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

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
    const findControllerBroker = vi.fn();
    const cluster = {
      addMultipleTargetTopics: vi.fn(),
      refreshMetadata: vi.fn(),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createAssignReplicasToDirsApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.assignReplicasToDirs({ brokerId: -1, replicas: [] })).rejects.toThrow(KafkaNonRetriableError);
    await expect(
      api.assignReplicasToDirs({
        brokerId: 1,
        replicas: [{ topic: 'events', partition: 0, directoryId: Buffer.from([1, 2, 3]) }],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
    expect(findControllerBroker).not.toHaveBeenCalled();
  });

  it('throws when a topic has no topic id', async () => {
    const findControllerBroker = vi.fn();
    const cluster = {
      addMultipleTargetTopics: vi.fn().mockResolvedValue(undefined),
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findTopicId: vi.fn().mockReturnValue(undefined),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createAssignReplicasToDirsApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.assignReplicasToDirs({
        brokerId: 1,
        replicas: [{ topic: 'events', partition: 0, directoryId: directoryA }],
      }),
    ).rejects.toThrow(/No topic id for topic events/);
    expect(findControllerBroker).not.toHaveBeenCalled();
  });
});
