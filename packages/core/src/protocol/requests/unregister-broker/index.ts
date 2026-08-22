import type { ProtocolFactory, RequestFamily } from '../index';
import { unregisterBrokerRequestV0 } from './v0/request';
import { unregisterBrokerResponseV0 } from './v0/response';

export type { UnregisterBrokerRequestV0Fields } from './v0/request';
export type { UnregisterBrokerResponseV0Body } from './v0/response';

export interface UnregisterBrokerOptions {
  brokerId: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<UnregisterBrokerOptions>>> = {
  0: (options) => ({
    request: unregisterBrokerRequestV0({ brokerId: options.brokerId }),
    response: unregisterBrokerResponseV0,
  }),
};

export const UnregisterBroker: RequestFamily<UnregisterBrokerOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no UnregisterBroker protocol for version ${version}`);
    return factory;
  },
});
