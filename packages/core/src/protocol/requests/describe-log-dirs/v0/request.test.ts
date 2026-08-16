import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeLogDirsRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-log-dirs/v0/request', () => {
  it('round-trips a v0 request with topics', async () => {
    const value = {
      topics: [
        { topic: 'orders', partitions: [0, 1] },
        { topic: 'payments', partitions: [0] },
      ],
    };

    const encoder = await describeLogDirsRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('encodes an empty topics list as a null array (all log dirs)', async () => {
    const encoder = await describeLogDirsRequestV0({ topics: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(-1).buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual({ topics: [] });
  });
});
