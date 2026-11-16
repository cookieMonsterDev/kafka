import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import {
  InitProducerId,
  type InitProducerIdOptions,
  type InitProducerIdResponse,
} from '../protocol/requests/init-producer-id/index';
import type { Retrier } from '../retry/index';
import { CONCURRENT_TRANSACTIONS_ERRORS, COORDINATOR_LOADING_ERRORS, unionOf } from './error-classifier';
import type { Cluster } from './index';
import { CONTROLLER, coordinatorTarget } from './run';

/** A coordinator still loading, or still ending the transactional id's previous transaction. */
const INIT_PRODUCER_ID_RETRY_ON = unionOf(COORDINATOR_LOADING_ERRORS, CONCURRENT_TRANSACTIONS_ERRORS);

export interface SendInitProducerIdContext {
  cluster: Cluster;
  retry: Retrier;
}

/**
 * Sends InitProducerId to the transaction coordinator of `transactionalId`, or to the controller
 * without one, since then any broker can allocate the producer id. Retries a moved controller or
 * coordinator (after a metadata refresh, so the next lookup reaches its new address), a loading
 * coordinator and a transaction still ending; any other error rejects as is.
 */
export function sendInitProducerId(
  { cluster, retry }: SendInitProducerIdContext,
  request: InitProducerIdOptions,
): Promise<InitProducerIdResponse> {
  const { transactionalId } = request;
  const target =
    transactionalId === null ? CONTROLLER : coordinatorTarget(transactionalId, COORDINATOR_TYPES.TRANSACTION);

  return cluster.run(target, (broker) => broker.send(InitProducerId, request), {
    retrier: retry,
    retryOn: INIT_PRODUCER_ID_RETRY_ON,
  });
}
