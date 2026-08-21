import { KafkaNonRetriableError } from '../errors';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import type { DescribeTransactionsState } from '../protocol/requests/describe-transactions/v0/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType } from './helpers';

export interface TransactionsApi {
  describeTransactions: (transactionalIds: string[]) => Promise<{ transactionStates: DescribeTransactionsState[] }>;
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

  return { describeTransactions };
}
