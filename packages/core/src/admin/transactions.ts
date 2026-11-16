import type { Broker } from '../broker/index';
import { REPLICA_UNAVAILABLE_ERRORS, STALE_COORDINATOR_ERRORS } from '../cluster/error-classifier';
import { sendInitProducerId } from '../cluster/init-producer-id';
import { coordinatorTarget, leaderTarget, nodeTarget } from '../cluster/run';
import { KafkaNonRetriableError, KafkaProtocolError } from '../errors';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import type { DescribeTransactionsState } from '../protocol/requests/describe-transactions/v0/response';
import type { ListTransactionsOptions } from '../protocol/requests/list-transactions/index';
import type { ListTransactionsState } from '../protocol/requests/list-transactions/v0/response';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { formatUnknown, groupByBroker } from './helpers';
import type {
  AbortTransactionOptions,
  FenceProducerResult,
  FenceProducersOptions,
  ForceTerminateTransactionOptions,
  ForceTerminateTransactionResult,
} from './types';
import {
  DescribeProducers,
  DescribeTransactions,
  ListTransactions,
  WriteTxnMarkers,
} from '../protocol/requests/lazy-families';

export interface TransactionsApi {
  describeTransactions: (transactionalIds: string[]) => Promise<{ transactionStates: DescribeTransactionsState[] }>;
  listTransactions: (options?: ListTransactionsOptions) => Promise<{ transactionStates: ListTransactionsState[] }>;
  fenceProducers: (options: FenceProducersOptions) => Promise<{ results: FenceProducerResult[] }>;
  abortTransaction: (options: AbortTransactionOptions) => Promise<void>;
  forceTerminateTransaction: (options: ForceTerminateTransactionOptions) => Promise<ForceTerminateTransactionResult>;
}

const DEFAULT_FENCE_TRANSACTION_TIMEOUT = 60_000;

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

/** The coordinator epoch to abort with: the given one, or the one the partition leader reports for `producerId`. */
async function resolveCoordinatorEpoch(
  leader: Broker,
  topic: string,
  partition: number,
  producerId: bigint,
  coordinatorEpoch?: number,
): Promise<number> {
  if (coordinatorEpoch !== undefined) return coordinatorEpoch >= 0 ? coordinatorEpoch : 0;

  const { topics } = await leader.send(DescribeProducers, { topics: [{ topic, partitions: [partition] }] });
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

export function createTransactionsApi({ cluster, retry }: AdminContext): TransactionsApi {
  const retrier = createRetrier(retry);

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

    return cluster.run(
      () =>
        transactionalIds.map((transactionalId) => coordinatorTarget(transactionalId, COORDINATOR_TYPES.TRANSACTION)),
      async (coordinators) => {
        const responses = await Promise.all(
          [...groupByBroker(transactionalIds, coordinators)].map(([coordinator, ids]) =>
            coordinator.send(DescribeTransactions, { transactionalIds: ids }),
          ),
        );
        return { transactionStates: responses.flatMap(({ transactionStates }) => transactionStates) };
      },
      { retrier, description: 'Could not describe transactions' },
    );
  };

  const listTransactions = async (
    options: ListTransactionsOptions = {},
  ): Promise<{ transactionStates: ListTransactionsState[] }> => {
    validateListTransactionsOptions(options);

    // ListTransactions reports a loading or unavailable transaction coordinator on that broker.
    return cluster.run(
      () => cluster.getNodeIds().map((nodeId) => nodeTarget(nodeId)),
      async (brokers) => {
        const listings = await Promise.all(
          brokers.map(async (broker) => {
            const { transactionStates } = await broker.send(ListTransactions, options);
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
      },
      { retrier, refreshMetadata: true, retryOn: STALE_COORDINATOR_ERRORS, description: 'Could not list transactions' },
    );
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

    const initProducerIdContext = { cluster, retry: retrier };
    const results = await Promise.all(
      transactionalIds.map(async (transactionalId): Promise<FenceProducerResult> => {
        try {
          // A fresh InitProducerId bumps the epoch, fencing the producer that holds the transactional id.
          const { producerId, producerEpoch } = await sendInitProducerId(initProducerIdContext, {
            transactionalId,
            transactionTimeout,
            producerId: -1n,
            producerEpoch: -1,
          });
          return { transactionalId, errorCode: 0, producerId, producerEpoch };
        } catch (error) {
          if (error instanceof KafkaProtocolError) return { transactionalId, errorCode: error.code ?? -1 };
          throw error;
        }
      }),
    );
    return { results };
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

    // A moved leader (stale metadata) or an unavailable replica is retried after a metadata refresh.
    return cluster.run(
      leaderTarget(topic, partition),
      async (leader) => {
        const resolvedCoordinatorEpoch = await resolveCoordinatorEpoch(
          leader,
          topic,
          partition,
          producerId,
          coordinatorEpoch,
        );
        await leader.send(WriteTxnMarkers, {
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
      },
      {
        retrier,
        retryOn: REPLICA_UNAVAILABLE_ERRORS,
        description: 'Could not abort transaction; refreshing metadata before retry',
      },
    );
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
