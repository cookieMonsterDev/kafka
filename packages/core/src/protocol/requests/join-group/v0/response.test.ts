import { describe, expect, it } from 'vitest';
import v0ResponseFixture from '../fixtures/v0-response.json' with { type: 'json' };
import { joinGroupResponseV0 } from './response';

describe('protocol/requests/join-group/v0/response', () => {
  it('decodes a real fixture', async () => {
    const data = await joinGroupResponseV0.decode(Buffer.from(v0ResponseFixture.data));

    const memberId = 'test-169029db29f2ebfe07c1-fe0d5338-804e-42fa-af6a-c8f7b2467c6e';
    expect(data).toEqual({
      errorCode: 0,
      generationId: 11,
      groupProtocol: 'default',
      leaderId: memberId,
      memberId,
      members: [{ memberId, memberMetadata: Buffer.from([0, 0]) }],
    });
    await expect(joinGroupResponseV0.parse(data)).resolves.toBeTruthy();
  });
});
