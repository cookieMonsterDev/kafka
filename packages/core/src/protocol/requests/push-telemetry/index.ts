import { COMPRESSION_TYPES } from '../../compression/index';
import type { ProtocolFactory, RequestFamily } from '../index';
import { pushTelemetryRequestV0 } from './v0/request';
import { pushTelemetryResponseV0 } from './v0/response';

export type { PushTelemetryRequestV0Fields } from './v0/request';
export type { PushTelemetryResponseV0Body } from './v0/response';

export interface PushTelemetryOptions {
  clientInstanceId: Buffer;
  subscriptionId: number;
  terminating?: boolean;
  compressionType?: number;
  metrics: Buffer;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<PushTelemetryOptions>>> = {
  0: (options) => ({
    request: pushTelemetryRequestV0({
      clientInstanceId: options.clientInstanceId,
      subscriptionId: options.subscriptionId,
      terminating: options.terminating ?? false,
      compressionType: options.compressionType ?? COMPRESSION_TYPES.None,
      metrics: options.metrics,
    }),
    response: pushTelemetryResponseV0,
  }),
};

export const PushTelemetry: RequestFamily<PushTelemetryOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no PushTelemetry protocol for version ${version}`);
    return factory;
  },
});
