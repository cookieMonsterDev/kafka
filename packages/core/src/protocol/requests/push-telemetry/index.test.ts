import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { COMPRESSION_TYPES } from '../../compression/index';
import { PushTelemetry } from './index';
import { requestSchema } from './v0/request';

describe('protocol/requests/push-telemetry', () => {
  it('registers version 0', () => {
    expect(PushTelemetry.versions).toEqual([0]);
  });

  it('defaults terminating to false and compression to None', async () => {
    const clientInstanceId = Buffer.alloc(16, 2);
    const metrics = Buffer.from([1, 2, 3]);
    const { request } = PushTelemetry.protocol({ version: 0 })({
      clientInstanceId,
      subscriptionId: 1,
      metrics,
    });
    expect(request.apiVersion).toBe(0);
    const encoded = await request.encode();
    expect(requestSchema.read(new Decoder(encoded.buffer))).toEqual({
      clientInstanceId,
      subscriptionId: 1,
      terminating: false,
      compressionType: COMPRESSION_TYPES.None,
      metrics,
    });
  });
});
