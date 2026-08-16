import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { deleteTopicsResponseV2 } from './response';

describe('protocol/requests/delete-topics/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await deleteTopicsResponseV2.decode(Buffer.from(v1ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topicErrors).toHaveLength(2);
    await expect(deleteTopicsResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
