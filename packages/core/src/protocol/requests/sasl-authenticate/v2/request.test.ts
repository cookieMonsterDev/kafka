import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { Encoder } from '../../../encoder';
import { saslAuthenticateRequestV1 } from '../v1/request';
import { saslAuthenticateRequestV2, requestSchema } from './request';

const payload = { authBytes: Buffer.from('n,,n=user,r=nonce') };

describe('protocol/requests/sasl-authenticate/v2/request', () => {
  it('encodes compact authBytes and a trailing TAG_BUFFER', async () => {
    const definition = saslAuthenticateRequestV2(payload);
    expect(definition.apiVersion).toBe(2);

    const encoder = await definition.encode();
    const expected = new Encoder().writeUVarIntBytes(payload.authBytes).writeUVarInt(0);

    expect(encoder.buffer).toEqual(expected.buffer);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(payload);
  });

  it('is not the non-flexible v1 encoding', async () => {
    const v2 = await saslAuthenticateRequestV2(payload).encode();
    const v1 = await saslAuthenticateRequestV1(payload).encode();
    expect(v2.buffer).not.toEqual(v1.buffer);
  });
});
