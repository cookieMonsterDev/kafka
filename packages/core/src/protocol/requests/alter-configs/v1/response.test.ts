import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { alterConfigsResponseV1 } from './response.js';

describe('protocol/requests/alter-configs/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await alterConfigsResponseV1.decode(Buffer.from(v0ResponseFixture.data));
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(0);
    expect(data.resources).toHaveLength(1);
    await expect(alterConfigsResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
