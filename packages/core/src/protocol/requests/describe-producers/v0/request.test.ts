import { describe, expect, it } from 'vitest';
import requestFixture from '../fixtures/v0-request.json' with { type: 'json' };
import { Decoder } from '../../../decoder';
import { describeProducersRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-producers/v0/request', () => {
  it('encodes the official flexible request shape', async () => {
    const value = { topics: [{ topic: 'orders', partitions: [0, 2] }] };
    const encoder = await describeProducersRequestV0(value).encode();

    expect(encoder.buffer).toEqual(Buffer.from(requestFixture.data));
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
