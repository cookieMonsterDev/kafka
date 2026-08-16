import {
  KafkaConnectionClosedError,
  KafkaInvalidVarIntError,
  KafkaInvariantViolation,
  KafkaMemberIdRequired,
  KafkaProtocolError,
} from '../errors';
import type { Logger } from '../loggers/index';
import type { ConnectionPool } from '../network/connection-pool';
import { asTypedSend } from '../network/connection';
import { COMPRESSION_TYPES } from '../protocol/compression/index';
import { COORDINATOR_TYPES } from '../protocol/enums/coordinator-types';
import { failure } from '../protocol/error-codes';
import { API_KEYS } from '../protocol/requests/api-keys';
import { lookup } from '../protocol/requests/index';
import type { BrokerVersions, ProtocolFactory, ProtocolResult } from '../protocol/requests/index';
import { Lock } from '../utils/lock';
import { shuffle } from '../utils/shuffle';

import { AddOffsetsToTxn } from '../protocol/requests/add-offsets-to-txn/index';
import type { AddOffsetsToTxnOptions } from '../protocol/requests/add-offsets-to-txn/index';
import type { AddOffsetsToTxnResponseV1Body } from '../protocol/requests/add-offsets-to-txn/v1/response';
import { AddPartitionsToTxn } from '../protocol/requests/add-partitions-to-txn/index';
import type { AddPartitionsToTxnOptions } from '../protocol/requests/add-partitions-to-txn/index';
import type { AddPartitionsToTxnResponseV1Body } from '../protocol/requests/add-partitions-to-txn/v1/response';
import { AlterConfigs } from '../protocol/requests/alter-configs/index';
import type { AlterConfigsOptions } from '../protocol/requests/alter-configs/index';
import type { AlterConfigsResponseV1Body } from '../protocol/requests/alter-configs/v1/response';
import { AlterPartitionReassignments } from '../protocol/requests/alter-partition-reassignments/index';
import type { AlterPartitionReassignmentsRequestV0Options } from '../protocol/requests/alter-partition-reassignments/v0/request';
import type { AlterPartitionReassignmentsResponseV0Body } from '../protocol/requests/alter-partition-reassignments/v0/response';
import { ApiVersions } from '../protocol/requests/api-versions/index';
import type { ApiVersionsResponseV1Body } from '../protocol/requests/api-versions/v1/response';
import { CreateAcls } from '../protocol/requests/create-acls/index';
import type { CreateAclsOptions } from '../protocol/requests/create-acls/index';
import type { CreateAclsResponseV1Body } from '../protocol/requests/create-acls/v1/response';
import { CreatePartitions } from '../protocol/requests/create-partitions/index';
import type { CreatePartitionsOptions } from '../protocol/requests/create-partitions/index';
import type { CreatePartitionsResponseV1Body } from '../protocol/requests/create-partitions/v1/response';
import { CreateTopics } from '../protocol/requests/create-topics/index';
import type { CreateTopicsOptions } from '../protocol/requests/create-topics/index';
import type { CreateTopicsResponseV3Body } from '../protocol/requests/create-topics/v3/response';
import { DeleteAcls } from '../protocol/requests/delete-acls/index';
import type { DeleteAclsOptions } from '../protocol/requests/delete-acls/index';
import type { DeleteAclsResponseV1Body } from '../protocol/requests/delete-acls/v1/response';
import { DeleteGroups } from '../protocol/requests/delete-groups/index';
import type { DeleteGroupsOptions } from '../protocol/requests/delete-groups/index';
import type { DeleteGroupsResponseV1Body } from '../protocol/requests/delete-groups/v1/response';
import { DeleteRecords } from '../protocol/requests/delete-records/index';
import type { DeleteRecordsOptions } from '../protocol/requests/delete-records/index';
import type { DeleteRecordsResponseV1Body } from '../protocol/requests/delete-records/v1/response';
import { DeleteTopics } from '../protocol/requests/delete-topics/index';
import type { DeleteTopicsOptions } from '../protocol/requests/delete-topics/index';
import type { DeleteTopicsResponseV1Body } from '../protocol/requests/delete-topics/v1/response';
import { DescribeAcls } from '../protocol/requests/describe-acls/index';
import type { DescribeAclsOptions } from '../protocol/requests/describe-acls/index';
import type { DescribeAclsResponseV1Body } from '../protocol/requests/describe-acls/v1/response';
import { DescribeConfigs } from '../protocol/requests/describe-configs/index';
import type { DescribeConfigsOptions } from '../protocol/requests/describe-configs/index';
import type { DescribeConfigsResponseV2Body } from '../protocol/requests/describe-configs/v2/response';
import { DescribeGroups } from '../protocol/requests/describe-groups/index';
import type { DescribeGroupsOptions } from '../protocol/requests/describe-groups/index';
import type { DescribeGroupsResponseV2Body } from '../protocol/requests/describe-groups/v2/response';
import { ElectLeaders } from '../protocol/requests/elect-leaders/index';
import type { ElectLeadersOptions } from '../protocol/requests/elect-leaders/index';
import type { ElectLeadersResponseV1Body } from '../protocol/requests/elect-leaders/v1/response';
import { EndTxn } from '../protocol/requests/end-txn/index';
import type { EndTxnOptions } from '../protocol/requests/end-txn/index';
import type { EndTxnResponseV1Body } from '../protocol/requests/end-txn/v1/response';
import { Fetch } from '../protocol/requests/fetch/index';
import type { FetchRequestOptions } from '../protocol/requests/fetch/shared';
import type { FetchResponseV11Body } from '../protocol/requests/fetch/v11/response';
import { FindCoordinator } from '../protocol/requests/find-coordinator/index';
import type { FindCoordinatorOptions } from '../protocol/requests/find-coordinator/index';
import type { FindCoordinatorResponseV2Body } from '../protocol/requests/find-coordinator/v2/response';
import { Heartbeat } from '../protocol/requests/heartbeat/index';
import type { HeartbeatOptions } from '../protocol/requests/heartbeat/index';
import type { HeartbeatResponseV2Body } from '../protocol/requests/heartbeat/v2/response';
import { IncrementalAlterConfigs } from '../protocol/requests/incremental-alter-configs/index';
import type { IncrementalAlterConfigsOptions } from '../protocol/requests/incremental-alter-configs/index';
import type { IncrementalAlterConfigsResponseV1Body } from '../protocol/requests/incremental-alter-configs/v1/response';
import { InitProducerId } from '../protocol/requests/init-producer-id/index';
import type { InitProducerIdOptions } from '../protocol/requests/init-producer-id/index';
import type { InitProducerIdResponseV1Body } from '../protocol/requests/init-producer-id/v1/response';
import { JoinGroup } from '../protocol/requests/join-group/index';
import type { JoinGroupOptions } from '../protocol/requests/join-group/index';
import type { JoinGroupResponseV5Body } from '../protocol/requests/join-group/v5/response';
import { LeaveGroup } from '../protocol/requests/leave-group/index';
import type { LeaveGroupOptions } from '../protocol/requests/leave-group/index';
import type { LeaveGroupResponseV3Body } from '../protocol/requests/leave-group/v3/response';
import { ListGroups } from '../protocol/requests/list-groups/index';
import type { ListGroupsOptions } from '../protocol/requests/list-groups/index';
import type { ListGroupsResponseV2Body } from '../protocol/requests/list-groups/v2/response';
import { ListOffsets } from '../protocol/requests/list-offsets/index';
import type { ListOffsetsOptions } from '../protocol/requests/list-offsets/index';
import type { ListOffsetsResponseV3Body } from '../protocol/requests/list-offsets/v3/response';
import { ListPartitionReassignments } from '../protocol/requests/list-partition-reassignments/index';
import type { ListPartitionReassignmentsRequestV0Options } from '../protocol/requests/list-partition-reassignments/v0/request';
import type { ListPartitionReassignmentsResponseV0Body } from '../protocol/requests/list-partition-reassignments/v0/response';
import { Metadata } from '../protocol/requests/metadata/index';
import type { MetadataOptions } from '../protocol/requests/metadata/index';
import type { MetadataResponseV9Body } from '../protocol/requests/metadata/v9/response';
import { OffsetCommit } from '../protocol/requests/offset-commit/index';
import type { OffsetCommitOptions } from '../protocol/requests/offset-commit/index';
import type { OffsetCommitResponseV4Body } from '../protocol/requests/offset-commit/v4/response';
import { OffsetDelete } from '../protocol/requests/offset-delete/index';
import type { OffsetDeleteOptions } from '../protocol/requests/offset-delete/index';
import type { OffsetDeleteResponseV1Body } from '../protocol/requests/offset-delete/v1/response';
import { OffsetFetch } from '../protocol/requests/offset-fetch/index';
import type { OffsetFetchOptions } from '../protocol/requests/offset-fetch/index';
import type { OffsetFetchResponseV4Body } from '../protocol/requests/offset-fetch/v4/response';
import { OffsetForLeaderEpoch } from '../protocol/requests/offset-for-leader-epoch/index';
import type { OffsetForLeaderEpochOptions } from '../protocol/requests/offset-for-leader-epoch/index';
import type { OffsetForLeaderEpochResponseV4Body } from '../protocol/requests/offset-for-leader-epoch/v4/response';
import { Produce } from '../protocol/requests/produce/index';
import type { ProduceRequestOptions } from '../protocol/requests/produce/shared';
import type { ProduceResponseV6Body } from '../protocol/requests/produce/v6/response';
import { SaslAuthenticate } from '../protocol/requests/sasl-authenticate/index';
import { SyncGroup } from '../protocol/requests/sync-group/index';
import type { SyncGroupOptions } from '../protocol/requests/sync-group/index';
import type { SyncGroupResponseV2Body } from '../protocol/requests/sync-group/v2/response';
import { TxnOffsetCommit } from '../protocol/requests/txn-offset-commit/index';
import type { TxnOffsetCommitOptions } from '../protocol/requests/txn-offset-commit/index';
import type { TxnOffsetCommitResponseV1Body } from '../protocol/requests/txn-offset-commit/v1/response';
import { AlterUserScramCredentials } from '../protocol/requests/alter-user-scram-credentials/index';
import type { AlterUserScramCredentialsOptions } from '../protocol/requests/alter-user-scram-credentials/index';
import type { AlterUserScramCredentialsResponseV0Body } from '../protocol/requests/alter-user-scram-credentials/v0/response';
import { DescribeUserScramCredentials } from '../protocol/requests/describe-user-scram-credentials/index';
import type { DescribeUserScramCredentialsOptions } from '../protocol/requests/describe-user-scram-credentials/index';
import type { DescribeUserScramCredentialsResponseV0Body } from '../protocol/requests/describe-user-scram-credentials/v0/response';
import { DescribeClientQuotas } from '../protocol/requests/describe-client-quotas/index';
import type { DescribeClientQuotasOptions } from '../protocol/requests/describe-client-quotas/index';
import type { DescribeClientQuotasResponseV1Body } from '../protocol/requests/describe-client-quotas/v1/response';
import { AlterClientQuotas } from '../protocol/requests/alter-client-quotas/index';
import type { AlterClientQuotasOptions } from '../protocol/requests/alter-client-quotas/index';
import type { AlterClientQuotasResponseV1Body } from '../protocol/requests/alter-client-quotas/v1/response';
import { DescribeLogDirs } from '../protocol/requests/describe-log-dirs/index';
import type { DescribeLogDirsOptions } from '../protocol/requests/describe-log-dirs/index';
import type { DescribeLogDirsResponseV2Body } from '../protocol/requests/describe-log-dirs/v2/response';
import { AlterReplicaLogDirs } from '../protocol/requests/alter-replica-log-dirs/index';
import type { AlterReplicaLogDirsOptions } from '../protocol/requests/alter-replica-log-dirs/index';
import type { AlterReplicaLogDirsResponseV2Body } from '../protocol/requests/alter-replica-log-dirs/v2/response';
import { DescribeCluster } from '../protocol/requests/describe-cluster/index';
import type { DescribeClusterOptions } from '../protocol/requests/describe-cluster/index';
import type { DescribeClusterResponseV2Body } from '../protocol/requests/describe-cluster/v2/response';

type LookupRequest = ReturnType<typeof lookup>;

const notInitializedLookup: LookupRequest = () => {
  throw new Error('Broker not connected');
};

/** Pre-flexible brokers answer ApiVersions v3+ with a v0/v1 body instead of UNSUPPORTED_VERSION. */
function isApiVersionsNegotiationFallback(error: unknown): boolean {
  if (error instanceof KafkaProtocolError) return error.type === 'UNSUPPORTED_VERSION';
  return error instanceof RangeError || error instanceof KafkaInvalidVarIntError;
}

export interface BrokerOptions {
  connectionPool: ConnectionPool;
  logger: Logger;
  nodeId?: number | null;
  versions?: BrokerVersions | null;
  authenticationTimeout?: number;
  allowAutoTopicCreation?: boolean;
}

/**
 * Each node in a Kafka cluster is called a broker. This class holds the high-level operations a
 * node can perform - one method per Kafka API, dispatched through `lookupRequest` (populated once
 * `connect()` negotiates `ApiVersions` with the real broker).
 */
export class Broker {
  connectionPool: ConnectionPool;
  nodeId: number | null;
  rootLogger: Logger;
  logger: Logger;
  versions: BrokerVersions | null;
  authenticationTimeout: number;
  allowAutoTopicCreation: boolean;
  brokerAddress: string;
  lock: Lock;
  lookupRequest: LookupRequest;

  constructor({
    connectionPool,
    logger,
    nodeId = null,
    versions = null,
    authenticationTimeout = 10_000,
    allowAutoTopicCreation = true,
  }: BrokerOptions) {
    this.connectionPool = connectionPool;
    this.nodeId = nodeId;
    this.rootLogger = logger;
    this.logger = logger.namespace('Broker');
    this.versions = versions;
    this.authenticationTimeout = authenticationTimeout;
    this.allowAutoTopicCreation = allowAutoTopicCreation;

    // The lock timeout has twice the connectionTimeout because the same timeout is used for the
    // first ApiVersions call.
    const lockTimeout = 2 * this.connectionPool.connectionTimeout + this.authenticationTimeout;
    this.brokerAddress = `${this.connectionPool.host}:${this.connectionPool.port}`;

    this.lock = new Lock({ timeout: lockTimeout, description: `connect to broker ${this.brokerAddress}` });
    this.lookupRequest = notInitializedLookup;
  }

  isConnected(): boolean {
    return this.connectionPool.sasl
      ? this.connectionPool.isConnected() && this.connectionPool.isAuthenticated()
      : this.connectionPool.isConnected();
  }

  async connect(): Promise<void> {
    await this.lock.acquire();
    try {
      if (this.isConnected()) return;

      const connection = await this.connectionPool.getConnection();

      if (!this.versions) {
        this.versions = await this.apiVersions();
      }
      this.connectionPool.setVersions(this.versions);

      this.lookupRequest = lookup(this.versions);

      if (connection.getSupportAuthenticationProtocol() === null) {
        let supportAuthenticationProtocol = false;
        try {
          this.lookupRequest(API_KEYS.SaslAuthenticate, SaslAuthenticate);
          supportAuthenticationProtocol = true;
        } catch {
          supportAuthenticationProtocol = false;
        }
        this.connectionPool.setSupportAuthenticationProtocol(supportAuthenticationProtocol);

        this.logger.debug('Verified support for SaslAuthenticate', {
          broker: this.brokerAddress,
          supportAuthenticationProtocol,
        });
      }

      await connection.authenticate();
    } finally {
      await this.lock.release();
    }
  }

  async disconnect(): Promise<void> {
    await this.connectionPool.destroy();
  }

  async apiVersions(): Promise<BrokerVersions> {
    let response: ApiVersionsResponseV1Body | undefined;
    const availableVersions = [...ApiVersions.versions].sort((a, b) => b - a);
    const oldestVersion = availableVersions[availableVersions.length - 1];

    // Find the best version implemented by the server. Flexible ApiVersions (v3+) uses a v2
    // request header that pre-KIP-482 brokers cannot parse; they reply with a non-flexible body
    // which throws RangeError on tagged-field decode instead of UNSUPPORTED_VERSION.
    for (const candidateVersion of availableVersions) {
      try {
        const apiVersions = ApiVersions.protocol({ version: candidateVersion });
        response = await this.#sendRequest<ApiVersionsResponseV1Body>({
          ...apiVersions({}),
          requestTimeout: this.connectionPool.connectionTimeout,
        });
        break;
      } catch (e) {
        if (candidateVersion !== oldestVersion && isApiVersionsNegotiationFallback(e)) {
          this.logger.debug('ApiVersions negotiation falling back', {
            broker: this.brokerAddress,
            candidateVersion,
            error: (e as Error).message,
          });
          continue;
        }
        throw e;
      }
    }

    if (!response) {
      throw new KafkaProtocolError({ message: 'API Versions not supported', retriable: false });
    }

    const versions: Record<number, { minVersion: number; maxVersion: number }> = {};
    for (const version of response.apiVersions) {
      versions[version.apiKey] = { minVersion: version.minVersion, maxVersion: version.maxVersion };
    }
    return versions;
  }

  async metadata(topics: string[] = []): Promise<MetadataResponseV9Body> {
    const metadata = this.lookupRequest<MetadataOptions>(API_KEYS.Metadata, Metadata);
    const shuffledTopics = shuffle(topics);
    const protocol = metadata({ topics: shuffledTopics, allowAutoTopicCreation: this.allowAutoTopicCreation });

    if (shuffledTopics.length > 0) {
      return this.#send(protocol);
    }

    // An empty topic list means "all topics". Parallel tests (and deletes in flight) can
    // put individual topics into UNKNOWN_TOPIC_OR_PARTITION; skip those rather than
    // failing the whole broker list.
    return this.#send({
      request: protocol.request,
      response: {
        decode: (rawData: Buffer) => protocol.response.decode(rawData),
        parse: async (data: unknown) => {
          const body = data as MetadataResponseV9Body;
          const topicMetadata = body.topicMetadata.filter(
            (topic) =>
              !failure(topic.topicErrorCode) &&
              !topic.partitionMetadata.some((partition) => failure(partition.partitionErrorCode)),
          );
          return { ...body, topicMetadata };
        },
      },
    });
  }

  /** Resolves `undefined` only for `acks: 0`, where the broker never writes a response to the wire. */
  async produce(options: ProduceRequestOptions): Promise<ProduceResponseV6Body | undefined> {
    const produce = this.lookupRequest<ProduceRequestOptions>(API_KEYS.Produce, Produce);
    return this.#sendRequest<ProduceResponseV6Body>(
      produce({ ...options, compression: options.compression ?? COMPRESSION_TYPES.None }),
    );
  }

  async fetch(options: FetchRequestOptions): Promise<FetchResponseV11Body> {
    const fetch = this.lookupRequest<FetchRequestOptions>(API_KEYS.Fetch, Fetch);

    // Shuffle topic-partitions to ensure fair response allocation across partitions (KIP-74).
    const flattenedTopicPartitions = options.topics.flatMap(({ topic, partitions }) =>
      partitions.map((partition) => ({ topic, partition })),
    );
    const shuffledTopicPartitions = shuffle(flattenedTopicPartitions);

    // Consecutive partitions for the same topic can be combined into a single `topic` entry.
    const consolidatedTopicPartitions: {
      topic: string;
      partitions: (typeof shuffledTopicPartitions)[number]['partition'][];
    }[] = [];
    for (const { topic, partition } of shuffledTopicPartitions) {
      const last = consolidatedTopicPartitions.at(-1);
      if (last && last.topic === topic) {
        last.partitions.push(partition);
      } else {
        consolidatedTopicPartitions.push({ topic, partitions: [partition] });
      }
    }

    return this.#send(fetch({ ...options, topics: consolidatedTopicPartitions }));
  }

  async heartbeat(options: HeartbeatOptions): Promise<HeartbeatResponseV2Body> {
    const heartbeat = this.lookupRequest<HeartbeatOptions>(API_KEYS.Heartbeat, Heartbeat);
    return this.#send(heartbeat(options));
  }

  async findGroupCoordinator(options: FindCoordinatorOptions): Promise<FindCoordinatorResponseV2Body> {
    const findCoordinator = this.lookupRequest<FindCoordinatorOptions>(API_KEYS.GroupCoordinator, FindCoordinator);
    return this.#send(
      findCoordinator({
        ...options,
        coordinatorKey: options.coordinatorKey ?? options.groupId,
        coordinatorType: options.coordinatorType ?? COORDINATOR_TYPES.GROUP,
      }),
    );
  }

  async joinGroup(options: JoinGroupOptions): Promise<JoinGroupResponseV5Body> {
    const joinGroup = this.lookupRequest<JoinGroupOptions>(API_KEYS.JoinGroup, JoinGroup);
    const makeRequest = (memberId = options.memberId): Promise<JoinGroupResponseV5Body> =>
      this.#send(joinGroup({ ...options, memberId }));

    try {
      return await makeRequest();
    } catch (error) {
      if (error instanceof KafkaMemberIdRequired && error.memberId != null) {
        return makeRequest(error.memberId);
      }
      throw error;
    }
  }

  async leaveGroup(options: LeaveGroupOptions): Promise<LeaveGroupResponseV3Body> {
    const leaveGroup = this.lookupRequest<LeaveGroupOptions>(API_KEYS.LeaveGroup, LeaveGroup);
    return this.#send(leaveGroup(options));
  }

  async syncGroup(options: SyncGroupOptions): Promise<SyncGroupResponseV2Body> {
    const syncGroup = this.lookupRequest<SyncGroupOptions>(API_KEYS.SyncGroup, SyncGroup);
    return this.#send(syncGroup(options));
  }

  async listOffsets(options: ListOffsetsOptions): Promise<ListOffsetsResponseV3Body> {
    const listOffsets = this.lookupRequest<ListOffsetsOptions>(API_KEYS.ListOffsets, ListOffsets);
    const result = await this.#send<{
      throttleTime?: number;
      clientSideThrottleTime?: number;
      responses: {
        topic: string;
        partitions: {
          partition: number;
          errorCode: number;
          timestamp?: bigint;
          offset?: bigint;
          offsets?: bigint[];
          leaderEpoch?: number;
        }[];
      }[];
    }>(listOffsets(options));

    // ListOffsets v0 returns `offsets[]`; v1+ returns a single `offset`. Normalize to `offset`.
    return {
      throttleTime: result.throttleTime ?? 0,
      clientSideThrottleTime: result.clientSideThrottleTime ?? 0,
      responses: result.responses.map((response) => ({
        topic: response.topic,
        partitions: response.partitions.map((partition) => ({
          partition: partition.partition,
          errorCode: partition.errorCode,
          timestamp: partition.timestamp ?? -1n,
          offset: partition.offsets != null ? (partition.offsets.at(-1) ?? -1n) : (partition.offset ?? -1n),
          leaderEpoch: partition.leaderEpoch ?? -1,
        })),
      })),
    };
  }

  async offsetForLeaderEpoch(options: OffsetForLeaderEpochOptions): Promise<OffsetForLeaderEpochResponseV4Body> {
    const offsetForLeaderEpoch = this.lookupRequest<OffsetForLeaderEpochOptions>(
      API_KEYS.OffsetForLeaderEpoch,
      OffsetForLeaderEpoch,
    );
    return this.#send(offsetForLeaderEpoch(options));
  }

  async offsetCommit(options: OffsetCommitOptions): Promise<OffsetCommitResponseV4Body> {
    const offsetCommit = this.lookupRequest<OffsetCommitOptions>(API_KEYS.OffsetCommit, OffsetCommit);
    return this.#send(offsetCommit(options));
  }

  async offsetFetch(options: OffsetFetchOptions): Promise<OffsetFetchResponseV4Body> {
    const offsetFetch = this.lookupRequest<OffsetFetchOptions>(API_KEYS.OffsetFetch, OffsetFetch);
    return this.#send(offsetFetch(options));
  }

  async describeGroups(options: DescribeGroupsOptions): Promise<DescribeGroupsResponseV2Body> {
    const describeGroups = this.lookupRequest<DescribeGroupsOptions>(API_KEYS.DescribeGroups, DescribeGroups);
    return this.#send(describeGroups(options));
  }

  async createTopics(options: CreateTopicsOptions): Promise<CreateTopicsResponseV3Body> {
    const createTopics = this.lookupRequest<CreateTopicsOptions>(API_KEYS.CreateTopics, CreateTopics);
    return this.#send(createTopics(options));
  }

  async createPartitions(options: CreatePartitionsOptions): Promise<CreatePartitionsResponseV1Body> {
    const createPartitions = this.lookupRequest<CreatePartitionsOptions>(API_KEYS.CreatePartitions, CreatePartitions);
    return this.#send(createPartitions(options));
  }

  async deleteTopics(options: DeleteTopicsOptions): Promise<DeleteTopicsResponseV1Body> {
    const deleteTopics = this.lookupRequest<DeleteTopicsOptions>(API_KEYS.DeleteTopics, DeleteTopics);
    return this.#send(deleteTopics(options));
  }

  async describeConfigs(options: DescribeConfigsOptions): Promise<DescribeConfigsResponseV2Body> {
    const describeConfigs = this.lookupRequest<DescribeConfigsOptions>(API_KEYS.DescribeConfigs, DescribeConfigs);
    return this.#send(describeConfigs(options));
  }

  async alterConfigs(options: AlterConfigsOptions): Promise<AlterConfigsResponseV1Body> {
    const alterConfigs = this.lookupRequest<AlterConfigsOptions>(API_KEYS.AlterConfigs, AlterConfigs);
    return this.#send(alterConfigs(options));
  }

  async incrementalAlterConfigs(
    options: IncrementalAlterConfigsOptions,
  ): Promise<IncrementalAlterConfigsResponseV1Body> {
    const incrementalAlterConfigs = this.lookupRequest<IncrementalAlterConfigsOptions>(
      API_KEYS.IncrementalAlterConfigs,
      IncrementalAlterConfigs,
    );
    return this.#send(incrementalAlterConfigs(options));
  }

  async electLeaders(options: ElectLeadersOptions): Promise<ElectLeadersResponseV1Body> {
    const electLeaders = this.lookupRequest<ElectLeadersOptions>(API_KEYS.ElectLeaders, ElectLeaders);
    return this.#send(electLeaders(options));
  }

  async describeUserScramCredentials(
    options: DescribeUserScramCredentialsOptions = {},
  ): Promise<DescribeUserScramCredentialsResponseV0Body> {
    const describeUserScramCredentials = this.lookupRequest<DescribeUserScramCredentialsOptions>(
      API_KEYS.DescribeUserScramCredentials,
      DescribeUserScramCredentials,
    );
    return this.#send(describeUserScramCredentials(options));
  }

  async alterUserScramCredentials(
    options: AlterUserScramCredentialsOptions,
  ): Promise<AlterUserScramCredentialsResponseV0Body> {
    const alterUserScramCredentials = this.lookupRequest<AlterUserScramCredentialsOptions>(
      API_KEYS.AlterUserScramCredentials,
      AlterUserScramCredentials,
    );
    return this.#send(alterUserScramCredentials(options));
  }

  async describeClientQuotas(options: DescribeClientQuotasOptions): Promise<DescribeClientQuotasResponseV1Body> {
    const describeClientQuotas = this.lookupRequest<DescribeClientQuotasOptions>(
      API_KEYS.DescribeClientQuotas,
      DescribeClientQuotas,
    );
    return this.#send(describeClientQuotas(options));
  }

  async alterClientQuotas(options: AlterClientQuotasOptions): Promise<AlterClientQuotasResponseV1Body> {
    const alterClientQuotas = this.lookupRequest<AlterClientQuotasOptions>(
      API_KEYS.AlterClientQuotas,
      AlterClientQuotas,
    );
    return this.#send(alterClientQuotas(options));
  }

  async describeLogDirs(options: DescribeLogDirsOptions = {}): Promise<DescribeLogDirsResponseV2Body> {
    const describeLogDirs = this.lookupRequest<DescribeLogDirsOptions>(API_KEYS.DescribeLogDirs, DescribeLogDirs);
    return this.#send(describeLogDirs(options));
  }

  async alterReplicaLogDirs(options: AlterReplicaLogDirsOptions): Promise<AlterReplicaLogDirsResponseV2Body> {
    const alterReplicaLogDirs = this.lookupRequest<AlterReplicaLogDirsOptions>(
      API_KEYS.AlterReplicaLogDirs,
      AlterReplicaLogDirs,
    );
    return this.#send(alterReplicaLogDirs(options));
  }

  async describeCluster(options: DescribeClusterOptions = {}): Promise<DescribeClusterResponseV2Body> {
    const describeCluster = this.lookupRequest<DescribeClusterOptions>(API_KEYS.DescribeCluster, DescribeCluster);
    return this.#send(describeCluster(options));
  }

  /** Fetches a PID and bumps the producer epoch. Request should be made to the transaction coordinator. */
  async initProducerId(options: InitProducerIdOptions): Promise<InitProducerIdResponseV1Body> {
    const initProducerId = this.lookupRequest<InitProducerIdOptions>(API_KEYS.InitProducerId, InitProducerId);
    return this.#send(initProducerId(options));
  }

  /** Marks a TopicPartition as participating in the transaction. Request should be made to the transaction coordinator. */
  async addPartitionsToTxn(options: AddPartitionsToTxnOptions): Promise<AddPartitionsToTxnResponseV1Body> {
    const addPartitionsToTxn = this.lookupRequest<AddPartitionsToTxnOptions>(
      API_KEYS.AddPartitionsToTxn,
      AddPartitionsToTxn,
    );
    return this.#send(addPartitionsToTxn(options));
  }

  /** Request should be made to the transaction coordinator. */
  async addOffsetsToTxn(options: AddOffsetsToTxnOptions): Promise<AddOffsetsToTxnResponseV1Body> {
    const addOffsetsToTxn = this.lookupRequest<AddOffsetsToTxnOptions>(API_KEYS.AddOffsetsToTxn, AddOffsetsToTxn);
    return this.#send(addOffsetsToTxn(options));
  }

  /** Persists the offsets in the `__consumer_offsets` topic. Request should be made to the consumer coordinator. */
  async txnOffsetCommit(options: TxnOffsetCommitOptions): Promise<TxnOffsetCommitResponseV1Body> {
    const txnOffsetCommit = this.lookupRequest<TxnOffsetCommitOptions>(API_KEYS.TxnOffsetCommit, TxnOffsetCommit);
    return this.#send(txnOffsetCommit(options));
  }

  /** Indicates the transaction should be committed or aborted. Request should be made to the transaction coordinator. */
  async endTxn(options: EndTxnOptions): Promise<EndTxnResponseV1Body> {
    const endTxn = this.lookupRequest<EndTxnOptions>(API_KEYS.EndTxn, EndTxn);
    return this.#send(endTxn(options));
  }

  async listGroups(options: ListGroupsOptions = {}): Promise<ListGroupsResponseV2Body> {
    const listGroups = this.lookupRequest<ListGroupsOptions>(API_KEYS.ListGroups, ListGroups);
    return this.#send(listGroups(options));
  }

  async deleteGroups(options: DeleteGroupsOptions): Promise<DeleteGroupsResponseV1Body> {
    const deleteGroups = this.lookupRequest<DeleteGroupsOptions>(API_KEYS.DeleteGroups, DeleteGroups);
    return this.#send(deleteGroups(options));
  }

  async offsetDelete(options: OffsetDeleteOptions): Promise<OffsetDeleteResponseV1Body> {
    const offsetDelete = this.lookupRequest<OffsetDeleteOptions>(API_KEYS.OffsetDelete, OffsetDelete);
    return this.#send(offsetDelete(options));
  }

  async deleteRecords(options: DeleteRecordsOptions): Promise<DeleteRecordsResponseV1Body> {
    const deleteRecords = this.lookupRequest<DeleteRecordsOptions>(API_KEYS.DeleteRecords, DeleteRecords);
    return this.#send(deleteRecords(options));
  }

  async createAcls(options: CreateAclsOptions): Promise<CreateAclsResponseV1Body> {
    const createAcls = this.lookupRequest<CreateAclsOptions>(API_KEYS.CreateAcls, CreateAcls);
    return this.#send(createAcls(options));
  }

  async describeAcls(options: DescribeAclsOptions): Promise<DescribeAclsResponseV1Body> {
    const describeAcls = this.lookupRequest<DescribeAclsOptions>(API_KEYS.DescribeAcls, DescribeAcls);
    return this.#send(describeAcls(options));
  }

  async deleteAcls(options: DeleteAclsOptions): Promise<DeleteAclsResponseV1Body> {
    const deleteAcls = this.lookupRequest<DeleteAclsOptions>(API_KEYS.DeleteAcls, DeleteAcls);
    return this.#send(deleteAcls(options));
  }

  async alterPartitionReassignments(
    options: AlterPartitionReassignmentsRequestV0Options,
  ): Promise<AlterPartitionReassignmentsResponseV0Body> {
    const alterPartitionReassignments = this.lookupRequest<AlterPartitionReassignmentsRequestV0Options>(
      API_KEYS.AlterPartitionReassignments,
      AlterPartitionReassignments,
    );
    return this.#send(alterPartitionReassignments(options));
  }

  async listPartitionReassignments(
    options: ListPartitionReassignmentsRequestV0Options,
  ): Promise<ListPartitionReassignmentsResponseV0Body> {
    const listPartitionReassignments = this.lookupRequest<ListPartitionReassignmentsRequestV0Options>(
      API_KEYS.ListPartitionReassignments,
      ListPartitionReassignments,
    );
    return this.#send(listPartitionReassignments(options));
  }

  /** Every family but `Produce` (with `acks: 0`) always gets a response - asserts that invariant so callers don't have to. */
  async #send<T>(protocolResult: ProtocolResult): Promise<T> {
    const result = await this.#sendRequest<T>(protocolResult);
    if (result === undefined) {
      throw new KafkaInvariantViolation('Broker request unexpectedly returned no response');
    }
    return result;
  }

  async #sendRequest<T>(protocolResult: ProtocolResult): Promise<T | undefined> {
    try {
      return await this.connectionPool.send<T>(asTypedSend<T>(protocolResult));
    } catch (e) {
      if (e instanceof KafkaConnectionClosedError) {
        await this.disconnect();
      }
      throw e;
    }
  }
}

export type { ProtocolFactory };
