import { describe, expect, it } from 'vitest';
import { deleteGroupsRequestV2 } from '../v2/request';
import { deleteGroupsRequestV3 } from './request';

const payload = { groupIds: ['g1', 'g2'] };

describe('protocol/requests/delete-groups/v3/request', () => {
  it('encodes the same body as v2 under api version 3', async () => {
    const definition = deleteGroupsRequestV3(payload);
    expect(definition.apiVersion).toBe(3);

    const v3 = await definition.encode();
    const v2 = await deleteGroupsRequestV2(payload).encode();
    expect(v3.buffer).toEqual(v2.buffer);
  });
});
