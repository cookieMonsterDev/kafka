import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createUnregisterBrokerApi } from './unregister-broker';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

describe('admin/unregister-broker', () => {
  it('targets the active controller with the broker id', async () => {
    const broker = { unregisterBroker: vi.fn().mockResolvedValue({ errorCode: 0, errorMessage: null }) };
    const cluster = {
      refreshMetadata: vi.fn().mockResolvedValue(undefined),
      findControllerBroker: vi.fn().mockResolvedValue(broker),
    } as unknown as Cluster;
    const api = createUnregisterBrokerApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.unregisterBroker({ brokerId: 2 })).resolves.toBeUndefined();
    expect(broker.unregisterBroker).toHaveBeenCalledWith({ brokerId: 2 });
  });

  it('rejects invalid broker ids before contacting the cluster', async () => {
    const findControllerBroker = vi.fn();
    const cluster = {
      refreshMetadata: vi.fn(),
      findControllerBroker,
    } as unknown as Cluster;
    const api = createUnregisterBrokerApi({
      cluster,
      logger: silentLogger.namespace('Admin'),
      rootLogger: silentLogger,
    });

    await expect(api.unregisterBroker({ brokerId: -1 })).rejects.toThrow(KafkaNonRetriableError);
    expect(findControllerBroker).not.toHaveBeenCalled();
  });
});
