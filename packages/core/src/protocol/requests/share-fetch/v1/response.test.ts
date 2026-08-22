import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { shareFetchResponseV1 } from './response';

const topicId = Buffer.from('0123456789abcdef');

describe('protocol/requests/share-fetch/v1/response', () => {
  it('decodes an empty partition response with KIP-219 throttle handling', async () => {
    const buffer = new Encoder()
      .writeInt32(6)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeInt32(30_000)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeInt32(1)
      .writeInt32(5)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarInt(0)
      .writeUVarInt(0).buffer;

    const data = await shareFetchResponseV1.decode(buffer);
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 6,
      errorCode: 0,
      errorMessage: null,
      acquisitionLockTimeoutMs: 30_000,
      responses: [
        {
          topicId,
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              acknowledgeErrorCode: 0,
              acknowledgeErrorMessage: null,
              currentLeader: { leaderId: 1, leaderEpoch: 5 },
              records: [],
              acquiredRecords: [],
            },
          ],
        },
      ],
      nodeEndpoints: [],
    });
    await expect(shareFetchResponseV1.parse(data)).resolves.toEqual(data);
  });
});
