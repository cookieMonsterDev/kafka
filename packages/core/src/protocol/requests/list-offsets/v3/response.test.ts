import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { listOffsetsResponseV3 } from './response';

describe('protocol/requests/list-offsets/v3/response', () => {
  it('decodes the v2 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await listOffsetsResponseV3.decode(Buffer.from(v2ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.responses).toHaveLength(1);
    await expect(listOffsetsResponseV3.parse(data)).resolves.toBe(data);
  });
});
