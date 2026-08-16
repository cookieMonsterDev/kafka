import type { ProtocolFactory, RequestFamily } from '../index';
import { txnOffsetCommitRequestV0 } from './v0/request';
import { txnOffsetCommitResponseV0 } from './v0/response';
import { txnOffsetCommitRequestV1 } from './v1/request';
import { txnOffsetCommitResponseV1 } from './v1/response';

export interface TxnOffsetCommitOptions {
  transactionalId: string;
  groupId: string;
  producerId: bigint;
  producerEpoch: number;
  topics: { topic: string; partitions: { partition: number; offset: bigint; metadata: string | null }[] }[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<TxnOffsetCommitOptions>>> = {
  0: (values) => ({ request: txnOffsetCommitRequestV0(values), response: txnOffsetCommitResponseV0 }),
  1: (values) => ({ request: txnOffsetCommitRequestV1(values), response: txnOffsetCommitResponseV1 }),
};

export const TxnOffsetCommit: RequestFamily<TxnOffsetCommitOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no TxnOffsetCommit protocol for version ${version}`);
    return factory;
  },
});
