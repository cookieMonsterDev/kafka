import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { unregisterBrokerRequestV0, requestSchema } from './request';

describe('protocol/requests/unregister-broker/v0/request', () => {
  it('round-trips a flexible v0 request', async () => {
    const value = { brokerId: 3 };

    const encoder = await unregisterBrokerRequestV0(value).encode();
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
