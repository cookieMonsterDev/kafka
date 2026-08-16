import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { initProducerIdResponseV2 } from './response';

describe('protocol/requests/init-producer-id/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await initProducerIdResponseV2.decode(Buffer.from(v0ResponseFixture.data));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      producerId: 1006n,
      producerEpoch: 0,
    });
    await expect(initProducerIdResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
