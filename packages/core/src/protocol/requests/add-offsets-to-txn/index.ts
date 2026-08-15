import type { ProtocolFactory, RequestFamily } from '../index.js';
import { addOffsetsToTxnRequestV0 } from './v0/request.js';
import { addOffsetsToTxnResponseV0 } from './v0/response.js';
import { addOffsetsToTxnRequestV1 } from './v1/request.js';
import { addOffsetsToTxnResponseV1 } from './v1/response.js';

export interface AddOffsetsToTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  groupId: string;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<AddOffsetsToTxnOptions>>> = {
  0: (values) => ({ request: addOffsetsToTxnRequestV0(values), response: addOffsetsToTxnResponseV0 }),
  1: (values) => ({ request: addOffsetsToTxnRequestV1(values), response: addOffsetsToTxnResponseV1 }),
};

export const AddOffsetsToTxn: RequestFamily<AddOffsetsToTxnOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no AddOffsetsToTxn protocol for version ${version}`);
    return factory;
  },
});
