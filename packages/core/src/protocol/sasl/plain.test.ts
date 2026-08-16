import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder.js';
import { plainRequest, plainResponse } from './plain.js';

const NUL = String.fromCharCode(0);

function readBytesOrThrow(decoder: Decoder): Buffer {
  const bytes = decoder.readBytes();
  if (bytes === null) throw new Error('expected a non-null bytes field');
  return bytes;
}

describe('protocol/sasl/plain', () => {
  it('encodes authzid<NUL>username<NUL>password as a length-prefixed bytes field', async () => {
    const buffer = await plainRequest({ username: 'user', password: 'pencil' }).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`${NUL}user${NUL}pencil`);
  });

  it('uses the given authorizationIdentity when present', async () => {
    const buffer = await plainRequest({
      authorizationIdentity: 'admin',
      username: 'user',
      password: 'pencil',
    }).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe(`admin${NUL}user${NUL}pencil`);
  });

  it('response decode/parse both resolve true without inspecting the bytes', async () => {
    await expect(plainResponse.decode()).resolves.toBe(true);
    await expect(plainResponse.parse()).resolves.toBe(true);
  });
});
