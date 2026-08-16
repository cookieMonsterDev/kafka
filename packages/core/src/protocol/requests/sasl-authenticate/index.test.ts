import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { Encoder } from '../../encoder';
import { SaslAuthenticate } from './index';

describe('protocol/requests/sasl-authenticate', () => {
  it('implements versions 0 through 2', () => {
    expect(SaslAuthenticate.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const { request } = SaslAuthenticate.protocol({ version: 1 })({ authBytes: Buffer.from('x') });
    expect(request.apiVersion).toBe(1);
  });

  it('compact-encodes the inner SASL payload when authBytes are length-prefixed', async () => {
    const payload = Buffer.from('n,,n=user,r=nonce');
    const prefixed = new Encoder().writeBytes(payload).buffer;
    const { request, response } = SaslAuthenticate.protocol({ version: 2 })({ authBytes: prefixed });

    const decoder = new Decoder((await request.encode()).buffer);
    expect(decoder.readUVarIntBytes()).toEqual(payload);
    expect(decoder.readUVarInt()).toBe(0);

    const wire = new Encoder()
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntBytes(payload)
      .writeInt64(3600000n)
      .writeUVarInt(0).buffer;
    const decoded = (await response.decode(wire)) as { authBytes: Buffer };
    expect(decoded.authBytes).toEqual(new Encoder().writeBytes(payload).buffer);
  });
});
