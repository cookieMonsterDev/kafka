import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { saslAuthenticateResponseV2 } from './response';

const authBytes = Buffer.from('r=nonce,s=salt,i=4096');

function encodeV2Response(): Buffer {
  return new Encoder()
    .writeInt16(0)
    .writeUVarIntString(null)
    .writeUVarIntBytes(authBytes)
    .writeInt64(3600000n)
    .writeUVarInt(0).buffer;
}

describe('protocol/requests/sasl-authenticate/v2/response', () => {
  it('decodes compact authBytes as the inner SASL payload', async () => {
    const data = await saslAuthenticateResponseV2.decode(encodeV2Response());

    expect(data).toEqual({
      errorCode: 0,
      errorMessage: null,
      authBytes,
      sessionLifetimeMs: 3600000n,
    });
    await expect(saslAuthenticateResponseV2.parse(data)).resolves.toBe(data);
  });
});
