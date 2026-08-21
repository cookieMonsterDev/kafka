import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { expireDelegationTokenRequestV0, requestSchema } from './request';

const hmac = Buffer.from([9, 8, 7]);
const payload = { hmac, expiryTimePeriodMs: -1n };

describe('protocol/requests/expire-delegation-token/v0/request', () => {
  it('encodes hmac bytes and expiry period', async () => {
    const encoder = await expireDelegationTokenRequestV0(payload).encode();
    const expected = new Encoder().writeBytes(hmac).writeInt64(-1n);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
