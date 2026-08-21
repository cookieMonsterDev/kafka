import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { expireDelegationTokenRequestV1 } from '../v1/request';
import { expireDelegationTokenRequestV2, requestSchema } from './request';

const hmac = Buffer.from([9, 8, 7]);
const payload = { hmac, expiryTimePeriodMs: -1n };

describe('protocol/requests/expire-delegation-token/v2/request', () => {
  it('encodes compact hmac bytes and a TAG_BUFFER', async () => {
    const encoder = await expireDelegationTokenRequestV2(payload).encode();
    const expected = new Encoder().writeUVarIntBytes(hmac).writeInt64(-1n).writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await expireDelegationTokenRequestV2(payload).encode();
    const v1 = await expireDelegationTokenRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
