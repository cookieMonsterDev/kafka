import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { createPartitionsResponseV1 } from './response';

describe('protocol/requests/create-partitions/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await createPartitionsResponseV1.decode(Buffer.from(v1ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topicErrors).toHaveLength(2);
    await expect(createPartitionsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
