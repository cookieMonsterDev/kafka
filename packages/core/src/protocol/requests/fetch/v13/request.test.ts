import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { requestSchemaV13 } from '../shared';
import { fetchRequestV13 } from './request';

const topicId = Buffer.from('0123456789abcdef');

const options = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 10_485_760,
  topics: [
    {
      topic: 'orders',
      topicId,
      partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }],
    },
  ],
  rackId: 'rack1',
};

describe('protocol/requests/fetch/v13/request', () => {
  it('encodes a topic UUID instead of a compact topic name', async () => {
    const definition = fetchRequestV13(options);
    expect(definition.apiVersion).toBe(13);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeInt32(-1)
      .writeInt32(100)
      .writeInt32(1)
      .writeInt32(10_485_760)
      .writeInt8(ISOLATION_LEVEL.READ_COMMITTED)
      .writeInt32(0)
      .writeInt32(-1)
      .writeUVarInt(2)
      .writeBuffer(topicId)
      .writeUVarInt(2)
      .writeInt32(0)
      .writeInt32(-1)
      .writeInt64(0n)
      .writeInt32(-1)
      .writeInt64(-1n)
      .writeInt32(1_048_576)
      .writeUVarInt(0)
      .writeUVarInt(0)
      .writeUVarInt(1)
      .writeUVarIntString('rack1')
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchemaV13.read(new Decoder(encoder.buffer))).toEqual({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 10_485_760,
      isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [
        {
          topicId,
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: -1,
              fetchOffset: 0n,
              lastFetchedEpoch: -1,
              logStartOffset: -1n,
              maxBytes: 1_048_576,
            },
          ],
        },
      ],
      forgottenTopics: [],
      rackId: 'rack1',
    });
  });

  it('rejects a request that has no usable topicId', async () => {
    const request = fetchRequestV13({
      replicaId: -1,
      maxWaitTime: 1,
      minBytes: 1,
      maxBytes: 1,
      topics: [{ topic: 'orders', partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 100 }] }],
    });
    await expect(request.encode()).rejects.toThrow(/requires a 16-byte topicId/);
  });

  it('encodes forgotten topics by UUID', async () => {
    const encoder = await fetchRequestV13({
      ...options,
      forgottenTopics: [{ topic: 'orders', topicId, partitions: [0, 1] }],
    }).encode();

    const decoded = requestSchemaV13.read(new Decoder(encoder.buffer));
    expect(decoded.forgottenTopics).toEqual([{ topicId, partitions: [0, 1] }]);
  });
});
