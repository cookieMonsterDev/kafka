import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { offsetCommitRequestV7, requestSchema } from './request';

const payload = {
  groupId: 'g1',
  groupGenerationId: 1,
  memberId: 'm1',
  groupInstanceId: 'instance-1' as string | null,
  topics: [
    {
      topic: 'orders',
      partitions: [{ partition: 0, offset: 42n, leaderEpoch: 3, metadata: 'meta' }],
    },
  ],
};

describe('protocol/requests/offset-commit/v7/request', () => {
  it('encodes groupInstanceId after memberId', async () => {
    const definition = offsetCommitRequestV7(payload);
    expect(definition.apiVersion).toBe(7);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeString('g1')
      .writeInt32(1)
      .writeString('m1')
      .writeString('instance-1')
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

  it('encodes a null groupInstanceId', async () => {
    const encoder = await offsetCommitRequestV7({ ...payload, groupInstanceId: null }).encode();
    const expected = new Encoder()
      .writeString('g1')
      .writeInt32(1)
      .writeString('m1')
      .writeString(null)
      .writeInt32(1)
      .writeString('orders')
      .writeInt32(1)
      .writeInt32(0)
      .writeInt64(42n)
      .writeInt32(3)
      .writeString('meta');

    expect(encoder.buffer).toEqual(expected.buffer);
  });
});
