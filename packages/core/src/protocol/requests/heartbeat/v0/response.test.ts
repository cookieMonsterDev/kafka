import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { heartbeatResponseV0 } from './response';

describe('protocol/requests/heartbeat/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await heartbeatResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0 });
    await expect(heartbeatResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
