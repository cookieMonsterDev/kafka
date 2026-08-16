import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { listOffsetsResponseV4 } from './response';

describe('protocol/requests/list-offsets/v4/response', () => {
  it('decodes the v2/v3 wire format with KIP-219 client-side throttle remap', async () => {
    const data = await listOffsetsResponseV4.decode(Buffer.from(v2ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.responses).toHaveLength(1);
    expect(data.responses[0]?.partitions[0]).toEqual({
      partition: 0,
      errorCode: 0,
      timestamp: -1n,
      offset: 1n,
    });
    await expect(listOffsetsResponseV4.parse(data)).resolves.toBe(data);
  });
});
