import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder.js';
import { Encoder } from '../encoder.js';
import { scramFinalMessageRequest, scramFirstMessageRequest, scramResponse } from './scram.js';

function readBytesOrThrow(decoder: Decoder): Buffer {
  const bytes = decoder.readBytes();
  if (bytes === null) throw new Error('expected a non-null bytes field');
  return bytes;
}

describe('protocol/sasl/scram', () => {
  it('encodes the first message as a length-prefixed bytes field', async () => {
    const buffer = await scramFirstMessageRequest({ clientFirstMessage: 'n,,n=user,r=rOprNGfwEbeRWgbNEkqO' }).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe('n,,n=user,r=rOprNGfwEbeRWgbNEkqO');
  });

  it('encodes the final message as a length-prefixed bytes field', async () => {
    const buffer = await scramFinalMessageRequest({ finalMessage: 'c=biws,r=rOprNGfwEbeRWgbNEkqO,p=abc' }).encode();
    const decoder = new Decoder(buffer);
    expect(readBytesOrThrow(decoder).toString()).toBe('c=biws,r=rOprNGfwEbeRWgbNEkqO,p=abc');
  });

  it('decodes and parses a server message into its r/s/i entries plus the original string', async () => {
    const serverMessage =
      'r=IQi00EZwusKw0Io7FoBfqg1c7im78cnh566cwt0watlspw4p,s=bHcyM3p5bWk5aXF4OWM3cmswZHM5N2w0cA==,i=8192';
    const wire = new Encoder().writeBytes(serverMessage).buffer;

    const decoded = await scramResponse.decode(wire);
    await expect(scramResponse.parse(decoded)).resolves.toEqual({
      original: serverMessage,
      r: 'IQi00EZwusKw0Io7FoBfqg1c7im78cnh566cwt0watlspw4p',
      s: 'bHcyM3p5bWk5aXF4OWM3cmswZHM5N2w0cA==',
      i: '8192',
    });
  });

  it('throws on a malformed server message entry', async () => {
    const wire = new Encoder().writeBytes('not-a-valid-entry').buffer;
    const decoded = await scramResponse.decode(wire);
    await expect(scramResponse.parse(decoded)).rejects.toThrow(RangeError);
  });
});
