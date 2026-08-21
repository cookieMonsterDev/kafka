import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenRequestV1 } from '../v1/request';
import { describeDelegationTokenRequestV2, requestSchema } from './request';

describe('protocol/requests/describe-delegation-token/v2/request', () => {
  it('encodes compact owner principals', async () => {
    const payload = { owners: [{ principalType: 'User', name: 'alice' }] };
    const encoder = await describeDelegationTokenRequestV2(payload).encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('User')
      .writeUVarIntString('alice')
      .writeUVarInt(0)
      .writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes null owners as compact null (describe all)', async () => {
    const encoder = await describeDelegationTokenRequestV2({ owners: null }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeUVarInt(0).writeUVarInt(0).buffer);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const payload = { owners: [{ principalType: 'User', name: 'alice' }] };
    const v2 = await describeDelegationTokenRequestV2(payload).encode();
    const v1 = await describeDelegationTokenRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
