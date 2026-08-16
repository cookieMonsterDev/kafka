import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listGroupsResponseV0 } from './response';

describe('protocol/requests/list-groups/v0/response', () => {
  it('decodes error_code and a list of groups', async () => {
    const wire = new Encoder().writeInt16(0).writeInt32(1).writeString('my-group').writeString('consumer').buffer;

    const data = await listGroupsResponseV0.decode(wire);
    expect(data).toEqual({ errorCode: 0, groups: [{ groupId: 'my-group', protocolType: 'consumer' }] });
    await expect(listGroupsResponseV0.parse(data)).resolves.toBeTruthy();
  });

  it('throws on a failure error code', async () => {
    const wire = new Encoder().writeInt16(35).writeInt32(0).buffer;
    const data = await listGroupsResponseV0.decode(wire);
    await expect(listGroupsResponseV0.parse(data)).rejects.toThrow(/version of API is not supported/);
  });
});
