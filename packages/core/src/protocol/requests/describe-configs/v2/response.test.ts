import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { describeConfigsResponseV2 } from './response.js';

describe('protocol/requests/describe-configs/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await describeConfigsResponseV2.decode(Buffer.from(v1ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.resources).toHaveLength(1);
    await expect(describeConfigsResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
