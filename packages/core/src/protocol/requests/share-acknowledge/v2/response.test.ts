import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { shareAcknowledgeResponseV2 } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-acknowledge/v2/response', () => {
  it('decodes AcquisitionLockTimeoutMs before topic responses', async () => {
    const buffer = new Encoder()
      .writeInt32(3)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeInt32(15_000)
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

    const data = await shareAcknowledgeResponseV2.decode(buffer);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 3,
      errorCode: 0,
      errorMessage: null,
      acquisitionLockTimeoutMs: 15_000,
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
    await expect(shareAcknowledgeResponseV2.parse(data)).resolves.toEqual(data);
  });
});
