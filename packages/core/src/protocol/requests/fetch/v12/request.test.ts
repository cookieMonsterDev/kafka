import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { ISOLATION_LEVEL } from '../../../enums/isolation-level';
import { fetchRequestV12, requestSchema } from './request';

const payload = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 10_485_760,
  isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
  sessionId: 0,
  sessionEpoch: -1,
  topics: [
    {
      topic: 'orders',
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
  forgottenTopics: [] as { topic: string; partitions: number[] }[],
  rackId: 'rack1',
};

describe('protocol/requests/fetch/v12/request', () => {
  it('encodes compact strings/arrays, lastFetchedEpoch, and TAG_BUFFERs', async () => {
    const definition = fetchRequestV12({
      replicaId: payload.replicaId,
      maxWaitTime: payload.maxWaitTime,
      minBytes: payload.minBytes,
      maxBytes: payload.maxBytes,
      topics: [
        {
          topic: 'orders',
          partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }],
        },
      ],
      rackId: 'rack1',
    });
    expect(definition.apiVersion).toBe(12);

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
      .writeUVarIntString('orders')
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
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('writes lastFetchedEpoch when provided', async () => {
    const encoder = await fetchRequestV12({
      replicaId: -1,
      maxWaitTime: 1,
      minBytes: 1,
      maxBytes: 1,
      topics: [
        {
          topic: 't',
          partitions: [{ partition: 1, fetchOffset: 9n, lastFetchedEpoch: 7, maxBytes: 100 }],
        },
      ],
    }).encode();

    const decoded = requestSchema.read(new Decoder(encoder.buffer));
    expect(decoded.topics[0]?.partitions[0]?.lastFetchedEpoch).toBe(7);
  });
});
