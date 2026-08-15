import { describe, expect, it } from 'vitest';
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' };
import { heartbeatResponseV3 } from './response.js';

describe('protocol/requests/heartbeat/v3/response', () => {
  it('decodes a real fixture (identical wire format to v2)', async () => {
    const data = await heartbeatResponseV3.decode(Buffer.from(v3ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0, throttleTime: 0, clientSideThrottleTime: 0 });
  });
});
