import { KafkaNonRetriableError } from '../errors';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import type { DescribeTransactionsState } from '../protocol/requests/describe-transactions/v0/response';
import type { ListTransactionsOptions } from '../protocol/requests/list-transactions/index';
import type { ListTransactionsState } from '../protocol/requests/list-transactions/v0/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';

export interface TransactionsApi {
  describeTransactions: (transactionalIds: string[]) => Promise<{ transactionStates: DescribeTransactionsState[] }>;
  listTransactions: (options?: ListTransactionsOptions) => Promise<{ transactionStates: ListTransactionsState[] }>;
}

function validateListTransactionsOptions(options: ListTransactionsOptions): void {
  const { stateFilters, producerIdFilters, durationFilter, transactionalIdPattern } = options;

  if (stateFilters !== undefined) {
    if (!Array.isArray(stateFilters)) {
      throw new KafkaNonRetriableError(`Invalid stateFilters array ${formatUnknown(stateFilters)}`);
    }
    if (stateFilters.some((state) => typeof state !== 'string')) {
      throw new KafkaNonRetriableError('State filters must be strings');
    }
  }

  if (producerIdFilters !== undefined) {
    if (!Array.isArray(producerIdFilters)) {
      throw new KafkaNonRetriableError(`Invalid producerIdFilters array ${formatUnknown(producerIdFilters)}`);
    }
    if (producerIdFilters.some((producerId) => typeof producerId !== 'bigint')) {
      throw new KafkaNonRetriableError('Producer IDs must be bigint');
    }
  }

  if (durationFilter !== undefined && typeof durationFilter !== 'bigint') {
    throw new KafkaNonRetriableError(`Invalid durationFilter ${formatUnknown(durationFilter)}`);
  }

  if (
    transactionalIdPattern !== undefined &&
    transactionalIdPattern !== null &&
    typeof transactionalIdPattern !== 'string'
  ) {
    throw new KafkaNonRetriableError(`Invalid transactionalIdPattern ${formatUnknown(transactionalIdPattern)}`);
  }
}

export function createTransactionsApi({ cluster, logger, retry }: AdminContext): TransactionsApi {
  const describeTransactions = async (
    transactionalIds: string[],
  ): Promise<{ transactionStates: DescribeTransactionsState[] }> => {
    if (!Array.isArray(transactionalIds)) {
      throw new KafkaNonRetriableError(`Invalid transactionalIds array ${formatUnknown(transactionalIds)}`);
    }
    if (transactionalIds.some((transactionalId) => typeof transactionalId !== 'string' || transactionalId === '')) {
      throw new KafkaNonRetriableError('Transactional IDs must be non-empty strings');
    }
    if (transactionalIds.length === 0) return { transactionStates: [] };

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        const coordinators = await Promise.all(
          transactionalIds.map(async (transactionalId) => ({
            transactionalId,
            coordinator: await cluster.findGroupCoordinator({
              groupId: transactionalId,
              coordinatorType: COORDINATOR_TYPES.TRANSACTION,
            }),
          })),
        );
        const transactionsByCoordinator = new Map<
          string,
          { coordinator: (typeof coordinators)[number]['coordinator']; transactionalIds: string[] }
        >();

        for (const { transactionalId, coordinator } of coordinators) {
          const nodeId = String(coordinator.nodeId);
          const existing = transactionsByCoordinator.get(nodeId);
          if (existing) {
            existing.transactionalIds.push(transactionalId);
          } else {
            transactionsByCoordinator.set(nodeId, { coordinator, transactionalIds: [transactionalId] });
          }
        }

        const responses = await Promise.all(
          [...transactionsByCoordinator.values()].map(({ coordinator, transactionalIds: ids }) =>
            coordinator.describeTransactions({ transactionalIds: ids }),
          ),
        );
        return { transactionStates: responses.flatMap(({ transactionStates }) => transactionStates) };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not describe transactions', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { transactionStates: [] };
      }
    });
  };

  const listTransactions = async (
    options: ListTransactionsOptions = {},
  ): Promise<{ transactionStates: ListTransactionsState[] }> => {
    validateListTransactionsOptions(options);

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const listings = await Promise.all(
          cluster.getNodeIds().map(async (nodeId) => {
            const broker = await cluster.findBroker({ nodeId });
            const { transactionStates } = await broker.listTransactions(options);
            return transactionStates;
          }),
        );

        const byTransactionalId = new Map<string, ListTransactionsState>();
        for (const listing of listings.flat()) {
          if (!byTransactionalId.has(listing.transactionalId)) {
            byTransactionalId.set(listing.transactionalId, listing);
          }
        }
        return { transactionStates: [...byTransactionalId.values()] };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not list transactions', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { transactionStates: [] };
      }
    });
  };

  return { describeTransactions, listTransactions };
}
