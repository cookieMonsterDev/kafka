import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { shareGroupDescribeResponseV1 } from './response';

const topicId = Buffer.from('0123456789abcdef');

function emptyAssignment(): Encoder {
  return new Encoder().writeUVarInt(1).writeUVarInt(0);
}

describe('protocol/requests/share-group-describe/v1/response', () => {
  it('decodes a described share group and member assignment', async () => {
    const buffer = new Encoder()
      .writeInt32(7)
      .writeUVarInt(2)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntString('g')
      .writeUVarIntString('STABLE')
      .writeInt32(3)
      .writeInt32(3)
      .writeUVarIntString('uniform')
      .writeUVarInt(2)
      .writeUVarIntString('member-1')
      .writeUVarIntString(null)
      .writeInt32(1)
      .writeUVarIntString('client')
      .writeUVarIntString('/127.0.0.1')
      .writeUVarInt(2)
      .writeUVarIntString('events')
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarIntString('events')
      .writeUVarInt(2)
      .writeInt32(0)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeBuffer(emptyAssignment().buffer)
      .writeUVarInt(0)
      .writeInt32(-2147483648)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await shareGroupDescribeResponseV1.decode(buffer);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 7,
      groups: [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'g',
          groupState: 'STABLE',
          groupEpoch: 3,
          assignmentEpoch: 3,
          assignorName: 'uniform',
          members: [
            {
              memberId: 'member-1',
              rackId: null,
              memberEpoch: 1,
              clientId: 'client',
              clientHost: '/127.0.0.1',
              subscribedTopicNames: ['events'],
              assignment: {
                topicPartitions: [{ topicId, topicName: 'events', partitions: [0] }],
              },
            },
          ],
          authorizedOperations: -2147483648,
        },
      ],
    });
    await expect(shareGroupDescribeResponseV1.parse(data)).resolves.toBe(data);
  });

  it('rejects a per-group protocol error', async () => {
    const buffer = new Encoder()
      .writeInt32(0)
      .writeUVarInt(2)
      .writeInt16(69)
      .writeUVarIntString('missing')
      .writeUVarIntString('g')
      .writeUVarIntString('')
      .writeInt32(0)
      .writeInt32(0)
      .writeUVarIntString('')
      .writeUVarInt(1)
      .writeInt32(-2147483648)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await shareGroupDescribeResponseV1.decode(buffer);
    await expect(shareGroupDescribeResponseV1.parse(data)).rejects.toMatchObject({
      type: 'GROUP_ID_NOT_FOUND',
    });
  });
});
