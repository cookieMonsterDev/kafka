import { describe, expect, it } from 'vitest';
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' };
import { leaveGroupResponseV1 } from './response.js';

describe('protocol/requests/leave-group/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await leaveGroupResponseV1.decode(Buffer.from(v1ResponseFixture.data));
    expect(data).toEqual({ throttleTime: 0, errorCode: 0 });
    await expect(leaveGroupResponseV1.parse(data)).resolves.toBeTruthy();
  });
});
