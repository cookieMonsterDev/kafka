import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { createTopicsResponseV3 } from './response.js';

describe('protocol/requests/create-topics/v3/response', () => {
  it('decodes the v2 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await createTopicsResponseV3.decode(Buffer.from(v2ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.topicErrors).toHaveLength(2);
    await expect(createTopicsResponseV3.parse(data)).resolves.toBeTruthy();
  });
});
