import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { offsetFetchResponseV4 } from './response';

describe('protocol/requests/offset-fetch/v4/response', () => {
  it('decodes the v3 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await offsetFetchResponseV4.decode(Buffer.from(v3ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.responses).toHaveLength(1);
    await expect(offsetFetchResponseV4.parse(data)).resolves.toBe(data);
  });
});
