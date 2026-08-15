import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { deleteRecordsResponseV1 } from './response.js';

describe('protocol/requests/delete-records/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const response = deleteRecordsResponseV1({ topics: [] });
    const data = await response.decode(Buffer.from(v0ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topics).toHaveLength(1);
    await expect(response.parse(data)).resolves.toBeTruthy();
  });
});
