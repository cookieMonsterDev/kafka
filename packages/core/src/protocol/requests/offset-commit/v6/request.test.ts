import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetCommitRequestV5 } from '../v5/request';
import { withDefaultMetadata } from '../shared';
import { offsetCommitRequestV6, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  groupGenerationId: 1,
  memberId: 'm1',
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3, metadata: 'meta' }],
    },
  ],
};

describe('protocol/requests/offset-commit/v6/request', () => {
  it('encodes leaderEpoch after offset and before metadata', async () => {
    const definition = offsetCommitRequestV6(payload);
    expect(definition.apiVersion).toBe(6);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeString('g1')
      .writeInt32(1)
      .writeString('m1')
      .writeInt32(1)
      .writeString('orders')
      .writeInt32(1)
      .writeInt32(0)
      .writeInt64(42n)
      .writeInt32(3)
      .writeString('meta');

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('defaults omitted leaderEpoch to -1 via withDefaultMetadata', async () => {
    const encoder = await offsetCommitRequestV6({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: withDefaultMetadata([{ topic: 'orders', partitions: [{ partition: 0, offset: 1n }] }]),
    }).encode();

    const expected = new Encoder()
      .writeString('g')
      .writeInt32(1)
      .writeString('m')
      .writeInt32(1)
      .writeString('orders')
      .writeInt32(1)
      .writeInt32(0)
      .writeInt64(1n)
      .writeInt32(-1)
      .writeString(null);

    expect(encoder.buffer).toEqual(expected.buffer);
  });

  it('is not the v5 encoding', async () => {
    const v6 = await offsetCommitRequestV6(payload).encode();
    const v5 = await offsetCommitRequestV5({
      groupId: payload.groupId,
      groupGenerationId: payload.groupGenerationId,
      memberId: payload.memberId,
      topics: payload.topics.map(({ topic, partitions }) => ({
        topic,
        partitions: partitions.map(({ partition, offset, metadata }) => ({ partition, offset, metadata })),
      })),
    }).encode();
    expect(v6.buffer).not.toEqual(v5.buffer);
  });
});
