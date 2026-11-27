import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema } from '../v2/request';
import { describeLogDirsRequestV3 } from './request';

describe('protocol/requests/describe-log-dirs/v3/request', () => {
  it('round-trips a v3 request (wire format identical to v2)', async () => {
    const value = { topics: [{ topic: 'orders', partitions: [0, 1] }] };

    const encoder = await describeLogDirsRequestV3(value).encode();
    expect(describeLogDirsRequestV3(value).apiVersion).toBe(3);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
