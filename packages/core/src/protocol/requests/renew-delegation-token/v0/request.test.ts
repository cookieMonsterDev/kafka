import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { renewDelegationTokenRequestV0, requestSchema } from './request';

const hmac = Buffer.from([9, 8, 7]);
const payload = { hmac, renewPeriodMs: -1n };

describe('protocol/requests/renew-delegation-token/v0/request', () => {
  it('encodes hmac bytes and renew period', async () => {
    const encoder = await renewDelegationTokenRequestV0(payload).encode();
    const expected = new Encoder().writeBytes(hmac).writeInt64(-1n);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
