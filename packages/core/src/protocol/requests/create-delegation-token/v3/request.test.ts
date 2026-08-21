import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { createDelegationTokenRequestV3, requestSchema } from './request';

describe('protocol/requests/create-delegation-token/v3/request', () => {
  it('encodes a nullable owner principal ahead of renewers', async () => {
    const payload = {
      ownerPrincipalType: 'User',
      ownerPrincipalName: 'bob',
      renewers: [{ principalType: 'User', name: 'alice' }],
      maxLifetimeMs: -1n,
    };
    const encoder = await createDelegationTokenRequestV3(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString('User')
      .writeUVarIntString('bob')
      .writeUVarInt(2)
      .writeUVarIntString('User')
      .writeUVarIntString('alice')
      .writeUVarInt(0)
      .writeInt64(-1n)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('encodes a null owner as compact nulls (default to the request principal)', async () => {
    const payload = {
      ownerPrincipalType: null,
      ownerPrincipalName: null,
      renewers: [],
      maxLifetimeMs: 60_000n,
    };
    const encoder = await createDelegationTokenRequestV3(payload).encode();
    const expected = new Encoder()
      .writeUVarIntString(null)
      .writeUVarIntString(null)
      .writeUVarInt(1)
      .writeInt64(60_000n)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });
});
