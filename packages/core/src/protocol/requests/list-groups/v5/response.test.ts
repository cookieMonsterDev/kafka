import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listGroupsResponseV5 } from './response';

function encodeV5Response(options: { throttleTime: number; errorCode?: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(options.errorCode ?? 0)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeUVarIntString('consumer')
    .writeUVarIntString('Stable')
    .writeUVarIntString('classic')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-groups/v5/response', () => {
  it('decodes groupType on each group and remaps throttleTime', async () => {
    const data = await listGroupsResponseV5.decode(encodeV5Response({ throttleTime: 3 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 3,
      errorCode: 0,
      groups: [{ groupId: 'g1', protocolType: 'consumer', groupState: 'Stable', groupType: 'classic' }],
    });
    await expect(listGroupsResponseV5.parse(data)).resolves.toBe(data);
  });

  it('throws the response-level error', async () => {
    const data = await listGroupsResponseV5.decode(encodeV5Response({ throttleTime: 0, errorCode: 14 }));
    await expect(listGroupsResponseV5.parse(data)).rejects.toMatchObject({ type: 'GROUP_LOAD_IN_PROGRESS' });
  });
});
