import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createRaftVotersApi } from './raft-voters';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const directoryId = Buffer.alloc(16, 4);

function makeApi(cluster: ReturnType<typeof fakeRunCluster>) {
  return createRaftVotersApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

describe('admin/raft-voters', () => {
  it('targets the active controller for addRaftVoter', async () => {
    const broker = withSend({ addRaftVoter: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) });
    const cluster = fakeRunCluster({ controller: broker });
    const api = makeApi(cluster);

    await expect(
      api.addRaftVoter({
        voterId: 4,
        voterDirectoryId: directoryId,
        listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
        ackWhenCommitted: false,
      }),
    ).resolves.toBeUndefined();

    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
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
    const broker = withSend({ removeRaftVoter: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) });
    const cluster = fakeRunCluster({ controller: broker });
    const api = makeApi(cluster);

    await expect(
      api.removeRaftVoter({
        clusterId: 'cluster-1',
        voterId: 4,
        voterDirectoryId: directoryId,
      }),
    ).resolves.toBeUndefined();

    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
    expect(broker.removeRaftVoter).toHaveBeenCalledWith({
      clusterId: 'cluster-1',
      voterId: 4,
      voterDirectoryId: directoryId,
    });
  });

  it('rejects invalid directory ids and listeners', async () => {
    const cluster = fakeRunCluster({ controller: withSend({}) });
    const api = makeApi(cluster);

    await expect(
      api.addRaftVoter({
        voterId: 4,
        voterDirectoryId: Buffer.from([1, 2, 3]),
        listeners: [],
      }),
    ).rejects.toThrow(KafkaNonRetriableError);
    expect(cluster.run).not.toHaveBeenCalled();
  });
});
