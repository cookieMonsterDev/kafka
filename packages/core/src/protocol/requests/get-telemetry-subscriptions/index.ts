import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { getTelemetrySubscriptionsRequestV0, ZERO_CLIENT_INSTANCE_ID } from './v0/request';
import { getTelemetrySubscriptionsResponseV0 } from './v0/response';

export type { GetTelemetrySubscriptionsRequestV0Fields } from './v0/request';
export type { GetTelemetrySubscriptionsResponseV0Body } from './v0/response';
export { ZERO_CLIENT_INSTANCE_ID } from './v0/request';

export interface GetTelemetrySubscriptionsOptions {
  /** All-zero UUID on the first request; afterwards the id the broker assigned. */
  clientInstanceId?: Buffer;
}

const VERSIONS = {
  0: (options) => ({
    request: getTelemetrySubscriptionsRequestV0({
      clientInstanceId: options.clientInstanceId ?? ZERO_CLIENT_INSTANCE_ID,
    }),
    response: getTelemetrySubscriptionsResponseV0,
  }),
} satisfies VersionFactories<GetTelemetrySubscriptionsOptions>;

export const GetTelemetrySubscriptions = defineRequestFamily(KAFKA_APIS.GetTelemetrySubscriptions, VERSIONS);
