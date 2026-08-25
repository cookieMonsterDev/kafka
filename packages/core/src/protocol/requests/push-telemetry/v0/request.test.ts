import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { pushTelemetryRequestV0, requestSchema } from './request';

describe('protocol/requests/push-telemetry/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = {
      clientInstanceId: Buffer.alloc(16, 1),
      subscriptionId: 7,
      terminating: true,
      compressionType: 1,
      metrics: Buffer.from('otlp'),
    };

    const encoder = await pushTelemetryRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
