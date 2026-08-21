import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { consumerGroupDescribeResponseV1 } from './response';

describe('protocol/requests/consumer-group-describe/v1/response', () => {
  it('decodes memberType after the target assignment', async () => {
    const buffer = new Encoder()
      .writeInt32(0)
      .writeUVarInt(2)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntString('g')
      .writeUVarIntString('STABLE')
      .writeInt32(1)
      .writeInt32(1)
      .writeUVarIntString('uniform')
      .writeUVarInt(2)
      .writeUVarIntString('member-1')
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeInt32(1)
      .writeUVarIntString('client')
      .writeUVarIntString('/127.0.0.1')
      .writeUVarInt(1)
      .writeUVarIntString(null)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeInt8(1)
      .writeUVarInt(0)
      .writeInt32(-2147483648)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await consumerGroupDescribeResponseV1.decode(buffer);
    expect(data.groups[0]?.members[0]).toMatchObject({
      memberId: 'member-1',
      memberType: 1,
      assignment: { topicPartitions: [] },
      targetAssignment: { topicPartitions: [] },
    });
  });
});
