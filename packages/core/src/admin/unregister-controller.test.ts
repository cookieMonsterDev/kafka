import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { CONTROLLER } from '../cluster/run';
import { fakeRunCluster } from '../cluster/test-run';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode } from '../protocol/error-codes';
import { createUnregisterControllerApi } from './unregister-controller';
import { withSend } from '../broker/test-send';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function makeApi(cluster: ReturnType<typeof fakeRunCluster>) {
  return createUnregisterControllerApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
  });
}

describe('admin/unregister-controller', () => {
  it('targets the active controller with the controller id', async () => {
    const broker = withSend({
      unregisterController: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }),
    });
    const cluster = fakeRunCluster({ controller: broker });
    const api = makeApi(cluster);

    await expect(api.unregisterController({ controllerId: 3001 })).resolves.toBeUndefined();
    expect(cluster.resolveTarget).toHaveBeenCalledWith(CONTROLLER);
    expect(broker.unregisterController).toHaveBeenCalledWith({ controllerId: 3001 });
  });

  it('surfaces CONTROLLER_ID_NOT_REGISTERED without retrying', async () => {
    const unregisterController = vi.fn().mockRejectedValue(createErrorFromCode(136));
    const cluster = fakeRunCluster({ controller: withSend({ unregisterController }) });
    const api = makeApi(cluster);

    await expect(api.unregisterController({ controllerId: 9 })).rejects.toMatchObject({
      type: 'CONTROLLER_ID_NOT_REGISTERED',
    });
    expect(unregisterController).toHaveBeenCalledTimes(1);
  });

  it.each([-1, 1.5, '3', undefined])('rejects controller id %p before contacting the cluster', async (controllerId) => {
    const cluster = fakeRunCluster({ controller: withSend({}) });
    const api = makeApi(cluster);

    await expect(api.unregisterController({ controllerId: controllerId as number })).rejects.toThrow(
      KafkaNonRetriableError,
    );
    expect(cluster.run).not.toHaveBeenCalled();
  });
});
