import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createUnregisterBrokerApi } from './unregister-broker';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: ReturnType<typeof fakeRunCluster>) {
  return createUnregisterBrokerApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

describe('admin/unregister-broker', () => {
  it('targets the active controller with the broker id', async () => {
    const broker = withSend({ unregisterBroker: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) });
    const cluster = fakeRunCluster({ controller: broker });
    const api = makeApi(cluster);

    await expect(api.unregisterBroker({ brokerId: 2 })).resolves.toBeUndefined();
    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
    expect(broker.unregisterBroker).toHaveBeenCalledWith({ brokerId: 2 });
  });

  it('rejects invalid broker ids before contacting the cluster', async () => {
    const cluster = fakeRunCluster({ controller: withSend({}) });
    const api = makeApi(cluster);

    await expect(api.unregisterBroker({ brokerId: -1 })).rejects.toThrow(KafkaNonRetriableError);
    expect(cluster.run).not.toHaveBeenCalled();
  });
});
