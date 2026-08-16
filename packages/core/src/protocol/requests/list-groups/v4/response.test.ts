import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { listGroupsResponseV4 } from './response';

function encodeV4Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeInt16(0)
    .writeUVarInt(2)
    .writeUVarIntString('g1')
    .writeUVarIntString('consumer')
    .writeUVarIntString('Stable')
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/list-groups/v4/response', () => {
  it('decodes groupState on each group and remaps throttleTime', async () => {
    const data = await listGroupsResponseV4.decode(encodeV4Response({ throttleTime: 3 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 3,
      errorCode: 0,
      groups: [{ groupId: 'g1', protocolType: 'consumer', groupState: 'Stable' }],
    });
    await expect(listGroupsResponseV4.parse(data)).resolves.toBe(data);
  });
});
