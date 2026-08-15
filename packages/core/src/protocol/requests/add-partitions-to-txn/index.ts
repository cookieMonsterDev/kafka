import type { ProtocolFactory, RequestFamily } from '../index.js';
import { addPartitionsToTxnRequestV0 } from './v0/request.js';
import { addPartitionsToTxnResponseV0 } from './v0/response.js';
import { addPartitionsToTxnRequestV1 } from './v1/request.js';
import { addPartitionsToTxnResponseV1 } from './v1/response.js';

export interface AddPartitionsToTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  topics: { topic: string; partitions: number[] }[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<AddPartitionsToTxnOptions>>> = {
  0: (values) => ({ request: addPartitionsToTxnRequestV0(values), response: addPartitionsToTxnResponseV0 }),
  1: (values) => ({ request: addPartitionsToTxnRequestV1(values), response: addPartitionsToTxnResponseV1 }),
};

export const AddPartitionsToTxn: RequestFamily<AddPartitionsToTxnOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AddPartitionsToTxn protocol for version ${version}`);
    return factory;
  },
});
