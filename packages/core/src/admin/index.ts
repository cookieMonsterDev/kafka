import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { createAclsApi } from './acls';
import { createApiVersionsApi } from './api-versions';
import { createConfigsApi } from './configs';
import { createDelegationTokensApi } from './delegation-tokens';
import { createFeaturesApi } from './features';
import { createGroupsApi } from './groups';
import { createMetadataQuorumApi } from './metadata-quorum';
import { CONNECT, DISCONNECT, events, unwrap, wrap, type AdminEventName } from './instrumentation-events';
import { createLogDirsApi } from './log-dirs';
import { createOffsetsApi } from './offsets';
import { createProducersApi } from './producers';
import { createQuotasApi } from './quotas';
import { createRaftVotersApi } from './raft-voters';
import { createReassignmentsApi } from './reassignments';
import { createScramApi } from './scram';
import { createShareGroupsApi } from './share-groups';
import { createStreamsGroupsApi } from './streams-groups';
import { createTopicsApi } from './topics';
import { createTransactionsApi } from './transactions';
import { createUnregisterBrokerApi } from './unregister-broker';
import { createUnregisterControllerApi } from './unregister-controller';
import { createAssignReplicasToDirsApi } from './assign-replicas-to-dirs';
import type { Admin, AdminOptions } from './types';

export type {
  ActiveProducerState,
  Admin,
  AdminOptions,
  AclEntry,
  AclFilter,
  ApiVersionDescription,
  BrokerApiVersionsDescription,
  ConsumerGroupDescription,
  ConsumerGroupMemberDescription,
  CreateDelegationTokenOptions,
  CreateDelegationTokenResult,
  DelegationToken,
  DescribeApiVersionsOptions,
  DescribeDelegationTokenOptions,
  DescribeFeaturesResult,
  DescribeMetadataQuorumResult,
  DescribeProducersOptions,
  AlterPartitionReassignmentsOptions,
  DescribeReplicaLogDirsReplica,
  DescribeReplicaLogDirsResult,
  DescribeTopicPartitionsCursor,
  DescribeTopicPartitionsOptions,
  DescribeTopicPartitionsPartition,
  DescribeTopicPartitionsResult,
  DescribeTopicPartitionsTopic,
  DescribeTopicPartitionsTopicInput,
  LogDirDescription,
  ExpireDelegationTokenOptions,
  FenceProducerResult,
  FenceProducersOptions,
  AbortTransactionOptions,
  ForceTerminateTransactionOptions,
  ForceTerminateTransactionResult,
  KafkaPrincipal,
  ListTransactionsOptions,
  PartitionProducerState,
  RenewDelegationTokenOptions,
  RemoveMembersFromConsumerGroupMember,
  RemoveMembersFromConsumerGroupOptions,
  RemoveMembersFromConsumerGroupResult,
  ShareGroupDescription,
  DescribeStreamsGroupsOptions,
  StreamsGroupDescription,
  StreamsGroupMemberDescription,
  StreamsTopologyDescription,
  ListShareGroupOffsetsOptions,
  AlterShareGroupOffsetsOptions,
  DeleteShareGroupOffsetsOptions,
  AddRaftVoterOptions,
  RemoveRaftVoterOptions,
  UnregisterBrokerOptions,
  UnregisterControllerOptions,
  AssignReplicasToDirsOptions,
  AssignReplicasToDirsReplica,
  TopicConfig,
  TopicOffset,
  TransactionDescription,
  TransactionListing,
  TransactionTopic,
} from './types';
export { events };

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `admin.events.${key}`)
  .join(', ');

/**
 * User-facing admin client: topics, groups, ACLs, configs, offsets, reassignments, quotas, log dirs, and tokens.
 *
 * @see https://kafka.apache.org/44/operations/basic-kafka-operations/
 */
export function createAdmin({
  cluster,
  logger: rootLogger,
  retry,
  instrumentationEmitter: rootInstrumentationEmitter,
}: AdminOptions): Admin {
  const logger = rootLogger.namespace('Admin');
  const instrumentationEmitter = rootInstrumentationEmitter ?? new InstrumentationEventEmitter();
  const context = { cluster, logger, rootLogger, retry };

  const offsets = createOffsetsApi(context);
  const topics = createTopicsApi(context, { fetchTopicOffsets: offsets.fetchTopicOffsets });
  const producers = createProducersApi(context);
  const configs = createConfigsApi(context);
  const groups = createGroupsApi(context);
  const acls = createAclsApi(context);
  const reassignments = createReassignmentsApi(context);
  const scram = createScramApi(context);
  const quotas = createQuotasApi(context);
  const logDirs = createLogDirsApi(context);
  const apiVersions = createApiVersionsApi(context);
  const features = createFeaturesApi(context);
  const metadataQuorum = createMetadataQuorumApi(context);
  const raftVoters = createRaftVotersApi(context);
  const transactions = createTransactionsApi(context);
  const delegationTokens = createDelegationTokensApi(context);
  const shareGroups = createShareGroupsApi(context);
  const streamsGroups = createStreamsGroupsApi(context);
  const unregisterBrokerApi = createUnregisterBrokerApi(context);
  const unregisterControllerApi = createUnregisterControllerApi(context);
  const assignReplicasToDirsApi = createAssignReplicasToDirsApi(context);

  const on = (
    eventName: AdminEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener => {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
    }

    return instrumentationEmitter.addListener(unwrap(eventName), (event: InstrumentationEvent<unknown>) => {
      const wrapped = { ...event, type: wrap(event.type) };
      Promise.resolve(listener(wrapped)).catch((error: unknown) => {
        const err = error as Error;
        logger.error(`Failed to execute listener: ${err.message}`, { eventName, stack: err.stack });
      });
    });
  };

  async function connect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
    instrumentationEmitter.emit(CONNECT, {});
  }

  async function disconnect({ signal }: ConnectOptions = {}): Promise<void> {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.disconnect(), signal);
    instrumentationEmitter.emit(DISCONNECT, {});
  }

  return {
    connect,
    disconnect,
    clientInstanceId: () => cluster.clientInstanceId(),
    ...topics,
    ...producers,
    ...offsets,
    ...configs,
    ...groups,
    ...acls,
    ...reassignments,
    ...scram,
    ...quotas,
    ...logDirs,
    ...apiVersions,
    ...features,
    ...metadataQuorum,
    ...raftVoters,
    ...unregisterBrokerApi,
    ...unregisterControllerApi,
    ...assignReplicasToDirsApi,
    ...transactions,
    ...delegationTokens,
    ...shareGroups,
    ...streamsGroups,
    on,
    logger: () => logger,
    events,
    [Symbol.asyncDispose]: disconnect,
  };
}
