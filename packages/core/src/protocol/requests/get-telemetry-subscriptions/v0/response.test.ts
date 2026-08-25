import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { getTelemetrySubscriptionsResponseV0, responseSchema } from './response';

describe('protocol/requests/get-telemetry-subscriptions/v0/response', () => {
  const ok = {
    throttleTime: 12,
    errorCode: 0,
    clientInstanceId: Buffer.alloc(16, 9),
    subscriptionId: 42,
    acceptedCompressionTypes: [0, 1],
    pushIntervalMs: 30_000,
    telemetryMaxBytes: 1024,
    deltaTemporality: true,
    requestedMetrics: ['kafka.client.', ''],
  };

  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const encoder = new Encoder();
    responseSchema.write(encoder, ok);
    const data = await getTelemetrySubscriptionsResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...ok,
      throttleTime: 0,
      clientSideThrottleTime: 12,
    });
    await expect(getTelemetrySubscriptionsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      getTelemetrySubscriptionsResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 41,
        clientInstanceId: Buffer.alloc(16),
        subscriptionId: 0,
        acceptedCompressionTypes: [],
        pushIntervalMs: 0,
        telemetryMaxBytes: 0,
        deltaTemporality: false,
        requestedMetrics: [],
      }),
    ).rejects.toMatchObject({ type: 'NOT_CONTROLLER' });
  });
});
