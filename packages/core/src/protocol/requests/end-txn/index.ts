import type { ProtocolFactory, RequestFamily } from '../index';
import { endTxnRequestV0 } from './v0/request';
import { endTxnResponseV0 } from './v0/response';
import { endTxnRequestV1 } from './v1/request';
import { endTxnResponseV1 } from './v1/response';
import { endTxnRequestV2 } from './v2/request';
import { endTxnResponseV2 } from './v2/response';
import { endTxnRequestV3 } from './v3/request';
import { endTxnResponseV3 } from './v3/response';

export interface EndTxnOptions {
  transactionalId: string;
  producerId: bigint;
  producerEpoch: number;
  transactionResult: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<EndTxnOptions>>> = {
  0: (values) => ({ request: endTxnRequestV0(values), response: endTxnResponseV0 }),
  1: (values) => ({ request: endTxnRequestV1(values), response: endTxnResponseV1 }),
  2: (values) => ({ request: endTxnRequestV2(values), response: endTxnResponseV2 }),
  3: (values) => ({ request: endTxnRequestV3(values), response: endTxnResponseV3 }),
};

export const EndTxn: RequestFamily<EndTxnOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no EndTxn protocol for version ${version}`);
    return factory;
  },
});
