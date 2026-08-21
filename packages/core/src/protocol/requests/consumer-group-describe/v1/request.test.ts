import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { consumerGroupDescribeRequestV1, requestSchema } from './request';

describe('protocol/requests/consumer-group-describe/v1/request', () => {
  it('is the same encoding as version 0 with apiVersion 1', async () => {
    const payload = { groupIds: ['g'], includeAuthorizedOperations: false };
    const encoder = await consumerGroupDescribeRequestV1(payload).encode();
    expect(encoder.buffer).toEqual(
      new Encoder().writeUVarInt(2).writeUVarIntString('g').writeBoolean(false).writeUVarInt(0).buffer,
    );
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
    expect(consumerGroupDescribeRequestV1(payload).apiVersion).toBe(1);
  });
});
