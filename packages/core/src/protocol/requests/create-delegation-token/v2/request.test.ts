import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { createDelegationTokenRequestV1 } from '../v1/request';
import { createDelegationTokenRequestV2, requestSchema } from './request';

const payload = {
  renewers: [{ principalType: 'User', name: 'alice' }],
  maxLifetimeMs: -1n,
};

describe('protocol/requests/create-delegation-token/v2/request', () => {
  it('encodes compact strings/arrays and a TAG_BUFFER on every struct', async () => {
    const definition = createDelegationTokenRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder()
      .writeUVarInt(2)
      .writeUVarIntString('User')
      .writeUVarIntString('alice')
      .writeUVarInt(0)
      .writeInt64(-1n)
      .writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await createDelegationTokenRequestV2(payload).encode();
    const v1 = await createDelegationTokenRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
