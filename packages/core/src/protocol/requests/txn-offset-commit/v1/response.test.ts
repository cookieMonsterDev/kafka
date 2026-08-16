import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { txnOffsetCommitResponseV1 } from './response';

describe('protocol/requests/txn-offset-commit/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await txnOffsetCommitResponseV1.decode(Buffer.from(v0ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topics).toHaveLength(1);
    await expect(txnOffsetCommitResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
