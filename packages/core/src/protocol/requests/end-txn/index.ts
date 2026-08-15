import type { ProtocolFactory, RequestFamily } from '../index.js';
import { endTxnRequestV0 } from './v0/request.js';
import { endTxnResponseV0 } from './v0/response.js';
import { endTxnRequestV1 } from './v1/request.js';
import { endTxnResponseV1 } from './v1/response.js';

export interface EndTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  transactionResult: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<EndTxnOptions>>> = {
  0: (values) => ({ request: endTxnRequestV0(values), response: endTxnResponseV0 }),
  1: (values) => ({ request: endTxnRequestV1(values), response: endTxnResponseV1 }),
};

export const EndTxn: RequestFamily<EndTxnOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no EndTxn protocol for version ${version}`);
    return factory;
  },
});
