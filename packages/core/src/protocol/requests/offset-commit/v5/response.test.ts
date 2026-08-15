import { describe, expect, it } from 'vitest';
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' };
import { offsetCommitResponseV5 } from './response.js';

describe('protocol/requests/offset-commit/v5/response', () => {
  it('decodes the v4 wire format', async () => {
    const data = await offsetCommitResponseV5.decode(Buffer.from(v5ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.responses).toHaveLength(1);
    await expect(offsetCommitResponseV5.parse(data)).resolves.toBe(data);
  });
});
