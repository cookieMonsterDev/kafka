import type { ProtocolFactory, RequestFamily } from '../index';
import { txnOffsetCommitRequestV0 } from './v0/request';
import { txnOffsetCommitResponseV0 } from './v0/response';
import { txnOffsetCommitRequestV1 } from './v1/request';
import { txnOffsetCommitResponseV1 } from './v1/response';
import { txnOffsetCommitRequestV2 } from './v2/request';
import { txnOffsetCommitResponseV2 } from './v2/response';
import { txnOffsetCommitRequestV3 } from './v3/request';
import { txnOffsetCommitResponseV3 } from './v3/response';

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

const VERSIONS: Readonly<Record<number, ProtocolFactory<TxnOffsetCommitOptions>>> = {
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
  3: (values) => ({
    request: txnOffsetCommitRequestV3({
      transactionalId: values.transactionalId,
      groupId: values.groupId,
      producerId: values.producerId,
      producerEpoch: values.producerEpoch,
      generationId: values.generationId ?? -1,
      memberId: values.memberId ?? '',
      groupInstanceId: values.groupInstanceId ?? null,
      topics: withLeaderEpoch(values.topics),
    }),
    response: txnOffsetCommitResponseV3,
  }),
};

export const TxnOffsetCommit: RequestFamily<TxnOffsetCommitOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no TxnOffsetCommit protocol for version ${version}`);
    return factory;
  },
});
