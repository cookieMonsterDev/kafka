import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { leaveGroupResponseV0 } from './response';

describe('protocol/requests/leave-group/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await leaveGroupResponseV0.decode(Buffer.from(v0ResponseFixture.data));
    expect(data).toEqual({ errorCode: 0 });
    await expect(leaveGroupResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
