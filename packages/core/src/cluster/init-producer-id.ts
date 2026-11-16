import type { Broker } from '../broker/index';
import type { Logger } from '../loggers/index';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import {
  InitProducerId,
  type InitProducerIdOptions,
  type InitProducerIdResponse,
} from '../protocol/requests/init-producer-id/index';
import type { Retrier } from '../retry/index';
import type { Cluster } from './index';

/**
 * Errors from a transaction coordinator that has moved, is still loading, or is still ending the
 * transactional id's previous transaction. Each is worth another attempt.
 */
const RETRIABLE_COORDINATOR_ERRORS: ReadonlySet<string> = new Set([
  'NOT_COORDINATOR_FOR_GROUP',
  'GROUP_COORDINATOR_NOT_AVAILABLE',
  'GROUP_LOAD_IN_PROGRESS',
  // The previous producer may have crashed mid-transaction; retrying lets the coordinator abort it.
  // https://github.com/apache/kafka/blob/201da0542726472d954080d54bc585b111aaf86f/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1001-L1002
  'CONCURRENT_TRANSACTIONS',
]);

/** Without a transactional id any broker can allocate the producer id; the controller is asked. */
async function findBroker(cluster: Cluster, transactionalId: string | null): Promise<Broker> {
  if (transactionalId === null) {
    await cluster.refreshMetadataIfNecessary();
    return cluster.findControllerBroker();
  }
  return cluster.findGroupCoordinator({ groupId: transactionalId, coordinatorType: COORDINATOR_TYPES.TRANSACTION });
}

export interface SendInitProducerIdContext {
  cluster: Cluster;
  retry: Retrier;
  logger: Logger;
}

/**
 * Sends InitProducerId to the transaction coordinator of `transactionalId`, or to the controller
 * without one. The broker is looked up again on every attempt, so a retry reaches a moved
 * coordinator. Retries only the coordinator errors above; any other error rejects as is.
 */
export function sendInitProducerId(
  { cluster, retry, logger }: SendInitProducerIdContext,
  request: InitProducerIdOptions,
): Promise<InitProducerIdResponse> {
  const { transactionalId } = request;

  return retry(async (bail, retryCount, retryTime) => {
    try {
      const broker = await findBroker(cluster, transactionalId);
      return await broker.send(InitProducerId, request);
    } catch (e) {
      const error = e as Error & { type?: string };
      if (error.type && RETRIABLE_COORDINATOR_ERRORS.has(error.type)) {
        logger.debug('InitProducerId failed on a transaction coordinator error, retrying', {
          error: error.message,
          type: error.type,
          transactionalId,
          retryCount,
          retryTime,
        });
        throw error;
      }

      bail(error);
      throw error;
    }
  });
}
