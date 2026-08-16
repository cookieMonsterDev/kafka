import type { ProtocolFactory, RequestFamily } from '../index';
import { initProducerIdRequestV0 } from './v0/request';
import { initProducerIdResponseV0 } from './v0/response';
import { initProducerIdRequestV1 } from './v1/request';
import { initProducerIdResponseV1 } from './v1/response';

export interface InitProducerIdOptions {
  transactionalId: string | null;
  transactionTimeout: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<InitProducerIdOptions>>> = {
  0: (values) => ({ request: initProducerIdRequestV0(values), response: initProducerIdResponseV0 }),
  1: (values) => ({ request: initProducerIdRequestV1(values), response: initProducerIdResponseV1 }),
};

export const InitProducerId: RequestFamily<InitProducerIdOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no InitProducerId protocol for version ${version}`);
    return factory;
  },
});
