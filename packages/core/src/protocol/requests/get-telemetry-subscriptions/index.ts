import type { ProtocolFactory, RequestFamily } from '../index';
import { getTelemetrySubscriptionsRequestV0, ZERO_CLIENT_INSTANCE_ID } from './v0/request';
import { getTelemetrySubscriptionsResponseV0 } from './v0/response';

export type { GetTelemetrySubscriptionsRequestV0Fields } from './v0/request';
export type { GetTelemetrySubscriptionsResponseV0Body } from './v0/response';
export { ZERO_CLIENT_INSTANCE_ID } from './v0/request';

export interface GetTelemetrySubscriptionsOptions {
  /** All-zero UUID on the first request; afterwards the id the broker assigned. */
  clientInstanceId?: Buffer;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<GetTelemetrySubscriptionsOptions>>> = {
  0: (options) => ({
    request: getTelemetrySubscriptionsRequestV0({
      clientInstanceId: options.clientInstanceId ?? ZERO_CLIENT_INSTANCE_ID,
    }),
    response: getTelemetrySubscriptionsResponseV0,
  }),
};

export const GetTelemetrySubscriptions: RequestFamily<GetTelemetrySubscriptionsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no GetTelemetrySubscriptions protocol for version ${version}`);
    return factory;
  },
});
