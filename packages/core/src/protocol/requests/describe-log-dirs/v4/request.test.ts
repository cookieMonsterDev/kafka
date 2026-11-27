import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema } from '../v2/request';
import { describeLogDirsRequestV4 } from './request';

describe('protocol/requests/describe-log-dirs/v4/request', () => {
  it('round-trips a v4 request (wire format identical to v2)', async () => {
    const value = { topics: [{ topic: 'orders', partitions: [0, 1] }] };

    const encoder = await describeLogDirsRequestV4(value).encode();
    expect(describeLogDirsRequestV4(value).apiVersion).toBe(4);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
