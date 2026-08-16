import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { createTopicsResponseV4 } from './response';

describe('protocol/requests/create-topics/v4/response', () => {
  it('decodes the v3 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await createTopicsResponseV4.decode(Buffer.from(v2ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topicErrors).toHaveLength(2);
    await expect(createTopicsResponseV4.parse(data)).resolves.toBeTruthy();
  });
});
