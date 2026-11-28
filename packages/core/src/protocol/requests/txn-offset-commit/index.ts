import { defineRequestFamily, highestVersionBelow, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { isUsableTopicId } from '../topic-ids';
import { txnOffsetCommitRequestV0 } from './v0/request';
import { txnOffsetCommitResponseV0 } from './v0/response';
import { txnOffsetCommitRequestV1 } from './v1/request';
import { txnOffsetCommitResponseV1 } from './v1/response';
import { txnOffsetCommitRequestV2 } from './v2/request';
import { txnOffsetCommitResponseV2 } from './v2/response';
import { txnOffsetCommitRequestV3 } from './v3/request';
import { txnOffsetCommitResponseV3 } from './v3/response';
import { txnOffsetCommitRequestV4 } from './v4/request';
import { txnOffsetCommitResponseV4 } from './v4/response';
import { txnOffsetCommitRequestV5 } from './v5/request';
import { txnOffsetCommitResponseV5 } from './v5/response';
import { txnOffsetCommitRequestV6 } from './v6/request';
import { txnOffsetCommitResponseV6 } from './v6/response';

/**
 * First TxnOffsetCommit version that adds the group's offsets to a transaction of transaction
 * version 2 itself (KIP-890 part 2).
 */
export const TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION = 5;

/** First TxnOffsetCommit version that addresses topics by id instead of name (KIP-1319). */
export const TXN_OFFSET_COMMIT_TOPIC_ID_MIN_VERSION = 6;

export interface TxnOffsetCommitPartition {
  partition: number;
  offset: bigint;
  metadata: string | null;
  leaderEpoch?: number;
}

export interface TxnOffsetCommitOptions {
  transactionalId: string;
  groupId: string;
  producerId: bigint;
  producerEpoch: number;
  /** Each topic's `topicId`, when known, lets v6 address the topic by id. */
  topics: { topic: string; topicId?: Buffer | undefined; partitions: TxnOffsetCommitPartition[] }[];
  /**
   * The generation of a classic group, or the member epoch of a consumer group. Defaults to -1,
   * which, with no member id or group instance id, the coordinator accepts without checking the
   * group's members.
   */
  generationId?: number;
  memberId?: string;
  groupInstanceId?: string | null;
  /**
   * Whether the transaction runs under transaction version 2. Only then is v5 or v6 sent, which
   * adds the group's offsets to the transaction without AddOffsetsToTxn. Defaults to false.
   */
  isTransactionV2Enabled?: boolean;
}

function withLeaderEpoch(topics: TxnOffsetCommitOptions['topics']) {
  return topics.map((topic) => ({
    ...topic,
    partitions: topic.partitions.map((partition) => ({
      partition: partition.partition,
      offset: partition.offset,
      metadata: partition.metadata,
      leaderEpoch: partition.leaderEpoch ?? -1,
    })),
  }));
}

/** The v3+ body, with the group member fields v3 added, unset by default. */
function withGroupMember(values: TxnOffsetCommitOptions) {
  return {
    transactionalId: values.transactionalId,
    groupId: values.groupId,
    producerId: values.producerId,
    producerEpoch: values.producerEpoch,
    generationId: values.generationId ?? -1,
    memberId: values.memberId ?? '',
    groupInstanceId: values.groupInstanceId ?? null,
    topics: withLeaderEpoch(values.topics),
  };
}

const VERSIONS = {
  0: (values) => ({ request: txnOffsetCommitRequestV0(values), response: txnOffsetCommitResponseV0 }),
  1: (values) => ({ request: txnOffsetCommitRequestV1(values), response: txnOffsetCommitResponseV1 }),
  2: (values) => ({
    request: txnOffsetCommitRequestV2({
      transactionalId: values.transactionalId,
      groupId: values.groupId,
      producerId: values.producerId,
      producerEpoch: values.producerEpoch,
      topics: withLeaderEpoch(values.topics),
    }),
    response: txnOffsetCommitResponseV2,
  }),
  3: (values) => ({ request: txnOffsetCommitRequestV3(withGroupMember(values)), response: txnOffsetCommitResponseV3 }),
  4: (values) => ({ request: txnOffsetCommitRequestV4(withGroupMember(values)), response: txnOffsetCommitResponseV4 }),
  5: (values) => ({ request: txnOffsetCommitRequestV5(withGroupMember(values)), response: txnOffsetCommitResponseV5 }),
  6: (values) => ({
    request: txnOffsetCommitRequestV6(withGroupMember(values)),
    response: txnOffsetCommitResponseV6(values.topics),
  }),
} satisfies VersionFactories<TxnOffsetCommitOptions>;

const TXN_OFFSET_COMMIT_VERSIONS: readonly number[] = Object.keys(VERSIONS).map(Number);

/** The first version `options` may not be sent with; see {@link TxnOffsetCommit}. */
function firstUnusableVersion(options: TxnOffsetCommitOptions): number {
  if (options.isTransactionV2Enabled !== true) return TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION;
  if (!options.topics.every(({ topicId }) => isUsableTopicId(topicId))) return TXN_OFFSET_COMMIT_TOPIC_ID_MIN_VERSION;
  return Number.POSITIVE_INFINITY;
}

/**
 * v4 (Kafka 3.8+) tells the broker the client handles TRANSACTION_ABORTABLE (KIP-890). Within a
 * transaction the broker reads v5+ as transaction version 2, so v5 is sent only when the
 * transaction runs under it, and v6 (Kafka 4.4+) only when every topic also has an id; otherwise
 * the highest version below applies, as in the Java client.
 */
export const TxnOffsetCommit = defineRequestFamily(KAFKA_APIS.TxnOffsetCommit, VERSIONS, {
  preferVersion: (options, version, advertised) => {
    const firstUnusable = firstUnusableVersion(options);
    return version < firstUnusable
      ? version
      : highestVersionBelow(KAFKA_APIS.TxnOffsetCommit, TXN_OFFSET_COMMIT_VERSIONS, firstUnusable, advertised);
  },
});
