import { COMPRESSION_TYPES } from '../../compression/index';
import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

const VERSIONS = {
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
} satisfies VersionFactories<PushTelemetryOptions>;

export const PushTelemetry = defineRequestFamily(KAFKA_APIS.PushTelemetry, VERSIONS);
