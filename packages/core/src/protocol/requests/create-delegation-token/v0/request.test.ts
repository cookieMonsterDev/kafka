import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { createDelegationTokenRequestV0, requestSchema } from './request';

const payload = {
  renewers: [{ principalType: 'User', name: 'alice' }],
  maxLifetimeMs: -1n,
};

describe('protocol/requests/create-delegation-token/v0/request', () => {
  it('encodes renewers and max lifetime', async () => {
    const encoder = await createDelegationTokenRequestV0(payload).encode();
    const expected = new Encoder().writeInt32(1).writeString('User').writeString('alice').writeInt64(-1n);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes an empty renewer list', async () => {
    const encoder = await createDelegationTokenRequestV0({ renewers: [], maxLifetimeMs: 3_600_000n }).encode();
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(0).writeInt64(3_600_000n).buffer);
  });
});
