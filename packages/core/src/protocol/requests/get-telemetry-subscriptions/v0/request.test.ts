import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { getTelemetrySubscriptionsRequestV0, requestSchema, ZERO_CLIENT_INSTANCE_ID } from './request';

describe('protocol/requests/get-telemetry-subscriptions/v0/request', () => {
  it('round-trips a zero client instance id', async () => {
    const value = { clientInstanceId: ZERO_CLIENT_INSTANCE_ID };
    const encoder = await getTelemetrySubscriptionsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('round-trips a non-zero client instance id', async () => {
    const value = { clientInstanceId: Buffer.alloc(16, 7) };
    const encoder = await getTelemetrySubscriptionsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
