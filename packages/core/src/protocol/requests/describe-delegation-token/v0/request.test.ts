import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { describeDelegationTokenRequestV0, requestSchema } from './request';

describe('protocol/requests/describe-delegation-token/v0/request', () => {
  it('encodes owner principals', async () => {
    const payload = { owners: [{ principalType: 'User', name: 'alice' }] };
    const encoder = await describeDelegationTokenRequestV0(payload).encode();
    const expected = new Encoder().writeInt32(1).writeString('User').writeString('alice');
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes an empty owner list as null (describe all)', async () => {
    const encoder = await describeDelegationTokenRequestV0({ owners: [] }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(-1).buffer);
  });
});
