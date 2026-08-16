import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeGroupsRequestV2 } from '../v2/request';
import { describeGroupsRequestV3, requestSchema } from './request';

const payload = { groupIds: ['g1', 'g2'], includeAuthorizedOperations: true };

describe('protocol/requests/describe-groups/v3/request', () => {
  it('round-trips groupIds plus includeAuthorizedOperations', async () => {
    const definition = describeGroupsRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const encoder = await definition.encode();
    const expected = new Encoder().writeArray(['g1', 'g2']).writeBoolean(true);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the v2 encoding (no includeAuthorizedOperations)', async () => {
    const v3 = await describeGroupsRequestV3(payload).encode();
    const v2 = await describeGroupsRequestV2({ groupIds: payload.groupIds }).encode();
    expect(v3.buffer).not.toEqual(v2.buffer);
  });
});
