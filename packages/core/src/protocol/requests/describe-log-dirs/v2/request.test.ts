import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { describeLogDirsRequestV2, requestSchema } from './request';

describe('protocol/requests/describe-log-dirs/v2/request', () => {
  it('round-trips a flexible v2 request, preserving a null topics list', async () => {
    const value = { topics: null };

    const encoder = await describeLogDirsRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });

  it('round-trips a flexible v2 request with topics', async () => {
    const value = {
      topics: [
        { topic: 'orders', partitions: [0, 1] },
        { topic: 'payments', partitions: [0] },
      ],
    };

    const encoder = await describeLogDirsRequestV2(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
