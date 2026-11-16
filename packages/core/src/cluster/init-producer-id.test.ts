import { describe, expect, it, vi } from 'vitest';
import type { KafkaProtocolError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import { retrier } from '../retry/index';
import type { Cluster } from './index';
import { sendInitProducerId } from './init-producer-id';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });
const response = { producerId: 1000n, producerEpoch: 1, errorCode: 0, throttleTime: 0, clientSideThrottleTime: 0 };

function errorOfType(type: string): KafkaProtocolError {
  return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
}

function fakeCluster() {
  const broker = { initProducerId: vi.fn().mockResolvedValue(response) };
  const cluster = {
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
  };
  return { broker, cluster };
}

function context(cluster: ReturnType<typeof fakeCluster>['cluster']) {
  return {
    cluster: cluster as unknown as Cluster,
    retry: retrier({ retries: 3, initialRetryTime: 1, maxRetryTime: 5 }),
    logger: silentLogger,
  };
}

describe('cluster/sendInitProducerId', () => {
  it('asks the controller when there is no transactional id', async () => {
    const { broker, cluster } = fakeCluster();
    const request = { transactionalId: null, transactionTimeout: 30_000, producerId: -1n, producerEpoch: -1 };

    await expect(sendInitProducerId(context(cluster), request)).resolves.toEqual(response);

    expect(cluster.refreshMetadataIfNecessary).toHaveBeenCalled();
    expect(cluster.findGroupCoordinator).not.toHaveBeenCalled();
    expect(broker.initProducerId).toHaveBeenCalledWith(request);
  });

  it('asks the transaction coordinator of the transactional id', async () => {
    const { broker, cluster } = fakeCluster();
    const request = { transactionalId: 'tx', transactionTimeout: 30_000, producerId: 7n, producerEpoch: 2 };

    await sendInitProducerId(context(cluster), request);

    expect(cluster.findGroupCoordinator).toHaveBeenCalledWith({
      groupId: 'tx',
      coordinatorType: COORDINATOR_TYPES.TRANSACTION,
    });
    expect(cluster.findControllerBroker).not.toHaveBeenCalled();
    expect(broker.initProducerId).toHaveBeenCalledWith(request);
  });

  it.each(['CONCURRENT_TRANSACTIONS', 'NOT_COORDINATOR_FOR_GROUP', 'GROUP_COORDINATOR_NOT_AVAILABLE'])(
    'looks the coordinator up again and retries after %s',
    async (type) => {
      const { broker, cluster } = fakeCluster();
      broker.initProducerId.mockRejectedValueOnce(errorOfType(type));

      await expect(
        sendInitProducerId(context(cluster), { transactionalId: 'tx', transactionTimeout: 30_000 }),
      ).resolves.toEqual(response);

      expect(broker.initProducerId).toHaveBeenCalledTimes(2);
      expect(cluster.findGroupCoordinator).toHaveBeenCalledTimes(2);
    },
  );

  it('rejects any other error as is, without retrying', async () => {
    const { broker, cluster } = fakeCluster();
    const error = errorOfType('TRANSACTIONAL_ID_AUTHORIZATION_FAILED');
    broker.initProducerId.mockRejectedValue(error);

    await expect(
      sendInitProducerId(context(cluster), { transactionalId: 'tx', transactionTimeout: 30_000 }),
    ).rejects.toBe(error);
    expect(broker.initProducerId).toHaveBeenCalledTimes(1);
  });
});
