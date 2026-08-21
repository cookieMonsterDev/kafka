import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { gssapiRequest, gssapiResponse } from './gssapi';

function readBytesOrThrow(decoder: Decoder): Buffer {
  const bytes = decoder.readBytes();
  if (bytes === null) throw new Error('expected a non-null bytes field');
  return bytes;
}

describe('protocol/sasl/gssapi', () => {
  it('encodes a GSS token as a length-prefixed bytes field', async () => {
    const token = Buffer.from([0x60, 0x0b, 0x06, 0x09]);
    const buffer = await gssapiRequest(token).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder)).toEqual(token);
  });

  it('encodes an empty token', async () => {
    const buffer = await gssapiRequest(Buffer.alloc(0)).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder)).toEqual(Buffer.alloc(0));
  });

  it('decodes a length-prefixed server token', async () => {
    const token = Buffer.from('gss-server-token');
    const wire = new Encoder().writeBytes(token).buffer;
    await expect(gssapiResponse.parse(await gssapiResponse.decode(wire))).resolves.toEqual(token);
  });

  it('treats a null bytes field as an empty token', async () => {
    const wire = new Encoder().writeBytes(null).buffer;
    await expect(gssapiResponse.parse(await gssapiResponse.decode(wire))).resolves.toEqual(Buffer.alloc(0));
  });
});
