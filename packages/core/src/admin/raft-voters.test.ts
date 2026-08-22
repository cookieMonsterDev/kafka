import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createRaftVotersApi } from './raft-voters';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const directoryId = Buffer.alloc(16, 4);

describe('admin/raft-voters', () => {
  it('targets the active controller for addRaftVoter', async () => {
    const broker = { addRaftVoter: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) };
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findControllerBroker: vi.fn().mockResolvedValue(broker),
    } as unknown as Cluster;
    const api = createRaftVotersApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.addRaftVoter({
        voterId: 4,
        voterDirectoryId: directoryId,
        listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
        ackWhenCommitted: false,
      }),
    ).resolves.toBeUndefined();

    expect(broker.addRaftVoter).toHaveBeenCalledWith({
      clusterId: undefined,
      timeoutMs: undefined,
      voterId: 4,
      voterDirectoryId: directoryId,
      listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
      ackWhenCommitted: false,
    });
  });

  it('targets the active controller for removeRaftVoter', async () => {
    const broker = { removeRaftVoter: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) };
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findControllerBroker: vi.fn().mockResolvedValue(broker),
    } as unknown as Cluster;
    const api = createRaftVotersApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.removeRaftVoter({
        clusterId: 'cluster-1',
        voterId: 4,
        voterDirectoryId: directoryId,
      }),
    ).resolves.toBeUndefined();

    expect(broker.removeRaftVoter).toHaveBeenCalledWith({
      clusterId: 'cluster-1',
      voterId: 4,
      voterDirectoryId: directoryId,
    });
  });

  it('rejects invalid directory ids and listeners', async () => {
    const findControllerBroker = vi.fn();
    const cluster = {
      refreshMetadata: vi.fn(),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createRaftVotersApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(
      api.addRaftVoter({
        voterId: 4,
        voterDirectoryId: Buffer.from([1, 2, 3]),
        listeners: [],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
    expect(findControllerBroker).not.toHaveBeenCalled();
  });
});
