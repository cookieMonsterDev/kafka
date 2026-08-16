import { describe, expect, it } from 'vitest';
import v4ResponseFixture from '../fixtures/v4-response.json' with { type: 'json' };
import { offsetCommitResponseV4 } from './response';

describe('protocol/requests/offset-commit/v4/response', () => {
  it('decodes the v3 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await offsetCommitResponseV4.decode(Buffer.from(v4ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.responses).toHaveLength(1);
    await expect(offsetCommitResponseV4.parse(data)).resolves.toBe(data);
  });
});
