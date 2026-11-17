import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { unregisterBrokerRequestV0 } from './v0/request';
import { unregisterBrokerResponseV0 } from './v0/response';

export type { UnregisterBrokerRequestV0Fields } from './v0/request';
export type { UnregisterBrokerResponseV0Body } from './v0/response';

export interface UnregisterBrokerOptions {
  brokerId: number;
}

const VERSIONS = {
  0: (options) => ({
    request: unregisterBrokerRequestV0({ brokerId: options.brokerId }),
    response: unregisterBrokerResponseV0,
  }),
} satisfies VersionFactories<UnregisterBrokerOptions>;

export const UnregisterBroker = defineRequestFamily(KAFKA_APIS.UnregisterBroker, VERSIONS);
