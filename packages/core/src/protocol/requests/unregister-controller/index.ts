import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { unregisterControllerRequestV0 } from './v0/request';
import { unregisterControllerResponseV0 } from './v0/response';

export type { UnregisterControllerRequestV0Fields } from './v0/request';
export type { UnregisterControllerResponseV0Body } from './v0/response';

export interface UnregisterControllerOptions {
  controllerId: number;
}

const VERSIONS = {
  0: (options) => ({
    request: unregisterControllerRequestV0({ controllerId: options.controllerId }),
    response: unregisterControllerResponseV0,
  }),
} satisfies VersionFactories<UnregisterControllerOptions>;

export const UnregisterController = defineRequestFamily(KAFKA_APIS.UnregisterController, VERSIONS);
