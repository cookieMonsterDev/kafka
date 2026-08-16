import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { deleteGroupsResponseV0 } from './response';

describe('protocol/requests/delete-groups/v0/response', () => {
  it('decodes per-group results, never throwing from parse', async () => {
    const wire = new Encoder()
      .writeInt32(0) // throttleTime
      .writeInt32(2) // results length
      .writeString('g1')
      .writeInt16(0)
      .writeString('g2')
      .writeInt16(
        69,
      ) // GROUP_ID_NOT_FOUND
    .buffer;

    const data = await deleteGroupsResponseV0.decode(wire);

    expect(data.throttleTime).toBe(0);
    expect(data.results).toHaveLength(2);
    expect(data.results[0]).toEqual({ groupId: 'g1', errorCode: 0 });
    expect(data.results[1]?.groupId).toBe('g2');
    expect(data.results[1]?.errorCode).toBe(69);
    expect(data.results[1]?.error).toBeInstanceOf(Error);

    await expect(deleteGroupsResponseV0.parse(data)).resolves.toBe(data);
  });
});
