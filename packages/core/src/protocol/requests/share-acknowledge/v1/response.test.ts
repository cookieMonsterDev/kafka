import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { shareAcknowledgeResponseV1 } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-acknowledge/v1/response', () => {
  it('decodes a partition response with KIP-219 throttle handling', async () => {
    const buffer = new Encoder()
      .writeInt32(3)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeInt32(1)
      .writeInt32(5)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await shareAcknowledgeResponseV1.decode(buffer);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 3,
      errorCode: 0,
      errorMessage: null,
      responses: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              currentLeader: { leaderId: 1, leaderEpoch: 5 },
            },
          ],
        },
      ],
      nodeEndpoints: [],
    });
    await expect(shareAcknowledgeResponseV1.parse(data)).resolves.toEqual(data);
  });
});
