import { describe, expect, it } from 'vitest';
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { heartbeatResponseV2 } from './response';

describe('protocol/requests/heartbeat/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await heartbeatResponseV2.decode(Buffer.from(v2ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0, throttleTime: 0, clientSideThrottleTime: 0 });
    await expect(heartbeatResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
