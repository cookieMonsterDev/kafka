import { defineRequestFamily, highestVersionBelow, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

/**
 * First TxnOffsetCommit version that adds the group's offsets to a transaction of transaction
 * version 2 itself (KIP-890 part 2).
 */
export const TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION = 5;

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
  topics: { topic: string; partitions: TxnOffsetCommitPartition[] }[];
  generationId?: number;
  memberId?: string;
  groupInstanceId?: string | null;
  /**
   * Whether the transaction runs under transaction version 2. Only then is v5 sent, which adds the
   * group's offsets to the transaction without AddOffsetsToTxn. Defaults to false.
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
} satisfies VersionFactories<TxnOffsetCommitOptions>;

const TXN_OFFSET_COMMIT_VERSIONS: readonly number[] = Object.keys(VERSIONS).map(Number);

/**
 * v4 (Kafka 3.8+) tells the broker the client handles TRANSACTION_ABORTABLE (KIP-890). Within a
 * transaction the broker reads v5 as transaction version 2, so v5 is sent only when the transaction
 * runs under it, as in the Java client.
 */
export const TxnOffsetCommit = defineRequestFamily(KAFKA_APIS.TxnOffsetCommit, VERSIONS, {
  preferVersion: (options, version, advertised) =>
    version < TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION || options.isTransactionV2Enabled === true
      ? version
      : highestVersionBelow(
          KAFKA_APIS.TxnOffsetCommit,
          TXN_OFFSET_COMMIT_VERSIONS,
          TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION,
          advertised,
        ),
});
