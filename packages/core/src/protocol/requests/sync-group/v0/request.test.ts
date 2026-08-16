import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { syncGroupRequestV0 } from './request';

describe('protocol/requests/sync-group/v0/request', () => {
  it('encodes group_id, generation_id, member_id, and the assignment array', async () => {
    const assignment = Buffer.from('assignment-bytes');
    const definition = syncGroupRequestV0({
      groupId: 'test-group',
      generationId: 1,
      memberId: 'member-1',
      groupAssignment: [{ memberId: 'member-1', memberAssignment: assignment }],
    });
    const encoder = await definition.encode();

    const expected = new Encoder()
      .writeString('test-group')
      .writeInt32(1)
      .writeString('member-1')
      .writeArray([new Encoder().writeString('member-1').writeBytes(assignment)], 'object').buffer;
    expect(encoder.buffer).toEqual(expected);
  });
});
