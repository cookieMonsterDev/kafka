import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { pushTelemetryResponseV0, responseSchema } from './response';

describe('protocol/requests/push-telemetry/v0/response', () => {
  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const encoder = new Encoder();
    responseSchema.write(encoder, { throttleTime: 5, errorCode: 0 });
    const data = await pushTelemetryResponseV0.decode(encoder.buffer);
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 5, errorCode: 0 });
    await expect(pushTelemetryResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on UNKNOWN_SUBSCRIPTION_ID', async () => {
    await expect(
      pushTelemetryResponseV0.parse({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 117 }),
    ).rejects.toMatchObject({ type: 'UNKNOWN_SUBSCRIPTION_ID' });
  });

  it('throws on TELEMETRY_TOO_LARGE', async () => {
    await expect(
      pushTelemetryResponseV0.parse({ throttleTime: 0, clientSideThrottleTime: 0, errorCode: 118 }),
    ).rejects.toMatchObject({ type: 'TELEMETRY_TOO_LARGE' });
  });
});
