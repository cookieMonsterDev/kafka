import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { GetTelemetrySubscriptions, ZERO_CLIENT_INSTANCE_ID } from './index';
import { requestSchema } from './v0/request';

describe('protocol/requests/get-telemetry-subscriptions', () => {
  it('registers version 0', () => {
    expect(GetTelemetrySubscriptions.versions).toEqual([0]);
  });

  it('defaults clientInstanceId to the all-zero UUID', async () => {
    const { request } = GetTelemetrySubscriptions.protocol({ version: 0 })({});
    expect(request.apiVersion).toBe(0);
    const encoded = await request.encode();
    expect(requestSchema.read(new Decoder(encoded.buffer))).toEqual({
      clientInstanceId: ZERO_CLIENT_INSTANCE_ID,
    });
  });
});
