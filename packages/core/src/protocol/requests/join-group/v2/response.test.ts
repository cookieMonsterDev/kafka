import { describe, expect, it } from 'vitest';
import v2AssignerMetadataFixture from '../fixtures/v2-assigner-metadata.json' with { type: 'json' };
import v2ResponseFixture from '../fixtures/v2-response.json' with { type: 'json' };
import { joinGroupResponseV2 } from './response.js';

describe('protocol/requests/join-group/v2/response', () => {
  it('decodes a real fixture, with throttleTime ahead of error_code', async () => {
    const data = await joinGroupResponseV2.decode(Buffer.from(v2ResponseFixture.data));

    const memberId =
      'test-b773bdb220aa2b862440-23702-2b1581f6-55ea-4af0-97f0-931d4f071111-68a2051d-7b30-4161-b920-89346d7b672b';
    expect(data).toEqual({
      throttleTime: 0,
      errorCode: 0,
      generationId: 1,
      groupProtocol: 'AssignerName',
      leaderId: memberId,
      memberId,
      members: [{ memberId, memberMetadata: Buffer.from(v2AssignerMetadataFixture.data) }],
    });
    await expect(joinGroupResponseV2.parse(data)).resolves.toBeTruthy();
  });
});
