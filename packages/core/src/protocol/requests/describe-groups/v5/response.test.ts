import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { describeGroupsResponseV5 } from './response';

const memberMetadata = Buffer.from([0, 0]);
const memberAssignment = Buffer.from('{}');

function encodeV5Response(options: { throttleTime: number }): Buffer {
  return new Encoder()
    .writeInt32(options.throttleTime)
    .writeUVarInt(2)
    .writeInt16(0)
    .writeUVarIntString('g1')
    .writeUVarIntString('Stable')
    .writeUVarIntString('consumer')
    .writeUVarIntString('range')
    .writeUVarInt(2)
    .writeUVarIntString('member-1')
    .writeUVarIntString('instance-1')
    .writeUVarIntString('client-1')
    .writeUVarIntString('/127.0.0.1')
    .writeUVarIntBytes(memberMetadata)
    .writeUVarIntBytes(memberAssignment)
    .writeUVarInt(0)
    .writeInt32(0)
    .writeUVarInt(0)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/describe-groups/v5/response', () => {
  it('decodes a flexible body and remaps throttleTime', async () => {
    const data = await describeGroupsResponseV5.decode(encodeV5Response({ throttleTime: 6 }));

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 6,
      groups: [
        {
          errorCode: 0,
          groupId: 'g1',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'range',
          members: [
            {
              memberId: 'member-1',
              groupInstanceId: 'instance-1',
              clientId: 'client-1',
              clientHost: '/127.0.0.1',
              memberMetadata,
              memberAssignment,
            },
          ],
          authorizedOperations: 0,
        },
      ],
    });
    await expect(describeGroupsResponseV5.parse(data)).resolves.toBe(data);
  });
});
