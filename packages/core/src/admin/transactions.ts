import { KafkaBrokerNotFound, KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import type { DescribeTransactionsState } from '../protocol/requests/describe-transactions/v0/response';
import type { ListTransactionsOptions } from '../protocol/requests/list-transactions/index';
import type { ListTransactionsState } from '../protocol/requests/list-transactions/v0/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, protocolType, requireMetadata } from './helpers';
import type {
  AbortTransactionOptions,
  FenceProducerResult,
  FenceProducersOptions,
  ForceTerminateTransactionOptions,
  ForceTerminateTransactionResult,
} from './types';

export interface TransactionsApi {
  describeTransactions: (transactionalIds: string[]) => Promise<{ transactionStates: DescribeTransactionsState[] }>;
  listTransactions: (options?: ListTransactionsOptions) => Promise<{ transactionStates: ListTransactionsState[] }>;
  fenceProducers: (options: FenceProducersOptions) => Promise<{ results: FenceProducerResult[] }>;
  abortTransaction: (options: AbortTransactionOptions) => Promise<void>;
  forceTerminateTransaction: (options: ForceTerminateTransactionOptions) => Promise<ForceTerminateTransactionResult>;
}

const DEFAULT_FENCE_TRANSACTION_TIMEOUT = 60_000;

const FENCE_PRODUCER_RETRIABLE_ERRORS = new Set([
  'CONCURRENT_TRANSACTIONS',
  'GROUP_LOAD_IN_PROGRESS',
  'NOT_COORDINATOR_FOR_GROUP',
  'GROUP_COORDINATOR_NOT_AVAILABLE',
  'COORDINATOR_NOT_AVAILABLE',
]);

const ABORT_TRANSACTION_RETRIABLE_ERRORS = new Set([
  'NOT_LEADER_OR_FOLLOWER',
  'NOT_LEADER_FOR_PARTITION',
  'REPLICA_NOT_AVAILABLE',
  'BROKER_NOT_AVAILABLE',
  'UNKNOWN_TOPIC_OR_PARTITION',
]);

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

async function resolveCoordinatorEpoch(
  cluster: AdminContext['cluster'],
  topic: string,
  partition: number,
  producerId: bigint,
  coordinatorEpoch?: number,
): Promise<number> {
  if (coordinatorEpoch !== undefined) return coordinatorEpoch >= 0 ? coordinatorEpoch : 0;

  await requireMetadata(cluster, { topics: [topic] });
  const partitionsByLeader = cluster.findLeaderForPartitions(topic, [partition]);
  const leaderEntry = Object.entries(partitionsByLeader).find(([, partitions]) => partitions.includes(partition));
  if (!leaderEntry) {
    throw new KafkaBrokerNotFound(`Could not find leader for ${topic} partition ${partition}`, { retriable: true });
  }

  const [nodeId] = leaderEntry;
  const broker = await cluster.findBroker({ nodeId });
  const { topics } = await broker.describeProducers({ topics: [{ topic, partitions: [partition] }] });
  const partitionState = topics.find(({ topic: topicName }) => topicName === topic)?.partitions[0];
  const producerState = partitionState?.activeProducers.find((producer) => producer.producerId === producerId);
  if (!producerState) {
    throw new KafkaNonRetriableError(
      `No active producer ${producerId} on ${topic} partition ${partition}; supply coordinatorEpoch explicitly`,
    );
  }

  if (producerState.coordinatorEpoch >= 0) {
    return producerState.coordinatorEpoch;
  }

  return 0;
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

  const fenceProducers = async ({
    transactionalIds,
    transactionTimeout = DEFAULT_FENCE_TRANSACTION_TIMEOUT,
  }: FenceProducersOptions): Promise<{ results: FenceProducerResult[] }> => {
    if (!Array.isArray(transactionalIds)) {
      throw new KafkaNonRetriableError(`Invalid transactionalIds array ${formatUnknown(transactionalIds)}`);
    }
    if (transactionalIds.some((transactionalId) => typeof transactionalId !== 'string' || transactionalId === '')) {
      throw new KafkaNonRetriableError('Transactional IDs must be non-empty strings');
    }
    if (!Number.isInteger(transactionTimeout) || transactionTimeout < 0 || transactionTimeout > 2_147_483_647) {
      throw new KafkaNonRetriableError(`Invalid transactionTimeout ${formatUnknown(transactionTimeout)}`);
    }
    if (transactionalIds.length === 0) return { results: [] };

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        const results = await Promise.all(
          transactionalIds.map(async (transactionalId): Promise<FenceProducerResult> =>
            retrier(retry)(async (bail, retryCount, retryTime) => {
              try {
                const coordinator = await cluster.findGroupCoordinator({
                  groupId: transactionalId,
                  coordinatorType: COORDINATOR_TYPES.TRANSACTION,
                });
                const response = await coordinator.initProducerId({
                  transactionalId,
                  transactionTimeout,
                  producerId: -1n,
                  producerEpoch: -1,
                });
                return {
                  transactionalId,
                  errorCode: 0,
                  producerId: response.producerId,
                  producerEpoch: response.producerEpoch,
                };
              } catch (error) {
                if (error instanceof KafkaProtocolError) {
                  const type = error.type;
                  if (type && FENCE_PRODUCER_RETRIABLE_ERRORS.has(type)) {
                    logger.debug('Retrying fenceProducers after retriable coordinator error', {
                      transactionalId,
                      type,
                      retryCount,
                      retryTime,
                    });
                    if (
                      type === 'NOT_COORDINATOR_FOR_GROUP' ||
                      type === 'GROUP_COORDINATOR_NOT_AVAILABLE' ||
                      type === 'COORDINATOR_NOT_AVAILABLE'
                    ) {
                      await cluster.refreshMetadata();
                    }
                    throw error;
                  }
                  return {
                    transactionalId,
                    errorCode: error.code ?? -1,
                  };
                }
                bail(error as Error);
                throw error;
              }
            }),
          ),
        );
        return { results };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not fence producers', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { results: [] };
      }
    });
  };

  const abortTransaction = async ({
    topic,
    partition,
    producerId,
    producerEpoch,
    coordinatorEpoch,
    transactionVersion,
  }: AbortTransactionOptions): Promise<void> => {
    if (typeof topic !== 'string' || topic === '') {
      throw new KafkaNonRetriableError(`Invalid topic ${formatUnknown(topic)}`);
    }
    if (!Number.isInteger(partition) || partition < 0) {
      throw new KafkaNonRetriableError(`Invalid partition ${formatUnknown(partition)}`);
    }
    if (typeof producerId !== 'bigint') {
      throw new KafkaNonRetriableError(`Invalid producerId ${formatUnknown(producerId)}`);
    }
    if (!Number.isInteger(producerEpoch) || producerEpoch < 0) {
      throw new KafkaNonRetriableError(`Invalid producerEpoch ${formatUnknown(producerEpoch)}`);
    }
    if (coordinatorEpoch !== undefined && (!Number.isInteger(coordinatorEpoch) || coordinatorEpoch < -1)) {
      throw new KafkaNonRetriableError(`Invalid coordinatorEpoch ${formatUnknown(coordinatorEpoch)}`);
    }
    if (
      transactionVersion !== undefined &&
      (!Number.isInteger(transactionVersion) || transactionVersion < 0 || transactionVersion > 127)
    ) {
      throw new KafkaNonRetriableError(`Invalid transactionVersion ${formatUnknown(transactionVersion)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        const resolvedCoordinatorEpoch = await resolveCoordinatorEpoch(
          cluster,
          topic,
          partition,
          producerId,
          coordinatorEpoch,
        );
        await requireMetadata(cluster, { topics: [topic] });
        const partitionsByLeader = cluster.findLeaderForPartitions(topic, [partition]);
        const leaderEntry = Object.entries(partitionsByLeader).find(([, partitions]) => partitions.includes(partition));
        if (!leaderEntry) {
          throw new KafkaBrokerNotFound(`Could not find leader for ${topic} partition ${partition}`, {
            retriable: true,
          });
        }

        const [nodeId] = leaderEntry;
        const broker = await cluster.findBroker({ nodeId });
        await broker.writeTxnMarkers({
          markers: [
            {
              producerId,
              producerEpoch,
              transactionResult: false,
              coordinatorEpoch: resolvedCoordinatorEpoch,
              ...(transactionVersion !== undefined ? { transactionVersion } : {}),
              topics: [{ topic, partitions: [partition] }],
            },
          ],
        });
      } catch (error) {
        const type = protocolType(error);
        if (type && ABORT_TRANSACTION_RETRIABLE_ERRORS.has(type)) {
          logger.warn('Could not abort transaction; refreshing metadata before retry', {
            topic,
            partition,
            type,
            retryCount,
            retryTime,
          });
          await cluster.refreshMetadata();
          throw error;
        }

        bail(error as Error);
      }
    });
  };

  const forceTerminateTransaction = async ({
    transactionalId,
    transactionTimeout,
  }: ForceTerminateTransactionOptions): Promise<ForceTerminateTransactionResult> => {
    if (typeof transactionalId !== 'string' || transactionalId === '') {
      throw new KafkaNonRetriableError('Transactional IDs must be non-empty strings');
    }

    const { results } = await fenceProducers({ transactionalIds: [transactionalId], transactionTimeout });
    const result = results[0];
    if (!result) {
      throw new KafkaNonRetriableError(`Fence result missing for transactional ID ${transactionalId}`);
    }
    return result;
  };

  return { describeTransactions, listTransactions, fenceProducers, abortTransaction, forceTerminateTransaction };
}
