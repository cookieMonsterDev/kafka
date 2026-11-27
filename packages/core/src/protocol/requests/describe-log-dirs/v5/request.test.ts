import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema } from '../v2/request';
import { describeLogDirsRequestV5 } from './request';

describe('protocol/requests/describe-log-dirs/v5/request', () => {
  it('round-trips a v5 request (wire format identical to v2)', async () => {
    const value = { topics: [{ topic: 'orders', partitions: [0, 1] }] };

    const encoder = await describeLogDirsRequestV5(value).encode();
    expect(describeLogDirsRequestV5(value).apiVersion).toBe(5);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
