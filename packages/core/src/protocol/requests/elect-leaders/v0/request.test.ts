import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { electLeadersRequestV0, requestSchema } from './request';

describe('protocol/requests/elect-leaders/v0/request', () => {
  it('round-trips a v0 request with topic partitions', async () => {
    const value = {
      timeout: 5000,
      topicPartitions: [
        { topic: 'orders', partitions: [0, 1] },
        { topic: 'payments', partitions: [0] },
      ],
    };

    const encoder = await electLeadersRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('encodes null topicPartitions as a null array (all partitions)', async () => {
    const encoder = await electLeadersRequestV0({ timeout: 1000, topicPartitions: null }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(-1).writeInt32(1000).buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ timeout: 1000, topicPartitions: null });
  });

  it('encodes an empty topicPartitions list as an empty array (no partitions)', async () => {
    const encoder = await electLeadersRequestV0({ timeout: 1000, topicPartitions: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(0).writeInt32(1000).buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ timeout: 1000, topicPartitions: [] });
  });
});
