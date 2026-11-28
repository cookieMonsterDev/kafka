import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { describeGroupsResponseV6 } from './response';

const memberMetadata = Buffer.from([0, 0]);
const memberAssignment = Buffer.from('{}');

function encodeStableGroup(encoder: Encoder): Encoder {
  return encoder
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarIntString('g1')
    .writeUVarIntString('Stable')
    .writeUVarIntString('consumer')
    .writeUVarIntString('range')
    .writeUVarInt(2)
    .writeUVarIntString('member-1')
    .writeUVarIntString(null)
    .writeUVarIntString('client-1')
    .writeUVarIntString('/127.0.0.1')
    .writeUVarIntBytes(memberMetadata)
    .writeUVarIntBytes(memberAssignment)
    .writeUVarInt(0)
    .writeInt32(0)
    .writeUVarInt(0);
}

function encodeUnknownGroup(encoder: Encoder, errorMessage: string | null): Encoder {
  return encoder
    .writeInt16(69)
    .writeUVarIntString(errorMessage)
    .writeUVarIntString('missing')
    .writeUVarIntString('Dead')
    .writeUVarIntString('')
    .writeUVarIntString('')
    .writeUVarInt(1)
    .writeInt32(-2147483648)
    .writeUVarInt(0);
}

describe('protocol/requests/describe-groups/v6/response', () => {
  it('decodes errorMessage on each group and remaps throttleTime', async () => {
    const rawData = encodeStableGroup(new Encoder().writeInt32(6).writeUVarInt(2)).writeUVarInt(0).buffer;
    const data = await describeGroupsResponseV6.decode(rawData);

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 6,
      groups: [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'g1',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'range',
          members: [
            {
              memberId: 'member-1',
              groupInstanceId: null,
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
    await expect(describeGroupsResponseV6.parse(data)).resolves.toBe(data);
  });

  it('throws GROUP_ID_NOT_FOUND with the broker message for an unknown group', async () => {
    const encoder = encodeStableGroup(new Encoder().writeInt32(0).writeUVarInt(3));
    const rawData = encodeUnknownGroup(encoder, 'Group missing not found.').writeUVarInt(0).buffer;
    const data = await describeGroupsResponseV6.decode(rawData);

    await expect(describeGroupsResponseV6.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
    await expect(describeGroupsResponseV6.parse(data)).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
      code: 69,
      retriable: false,
      message: 'Group missing not found.',
    });
  });

  it('falls back to the generic message when the broker sends none', async () => {
    const rawData = encodeUnknownGroup(new Encoder().writeInt32(0).writeUVarInt(2), null).writeUVarInt(0).buffer;
    const data = await describeGroupsResponseV6.decode(rawData);

    await expect(describeGroupsResponseV6.parse(data)).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
      message: 'The group id was not found',
    });
  });
});
