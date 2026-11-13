import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder';
import { encodeHeader } from './header';

// Headers are decoded as part of a record (`decodeRecord` in record.ts); these tests read the
// encoded key and value back with the matching Decoder primitives.
function readHeader(encoded: Buffer): { key: string | null; value: Buffer | null } {
  const decoder = new Decoder(encoded);
  return { key: decoder.readVarIntString(), value: decoder.readVarIntBytes() };
}

describe('protocol/records/header', () => {
  it('encodes a key/value pair', () => {
    const encoded = encodeHeader({ key: 'header-key-0', value: Buffer.from('header-value-0') }).buffer;
    expect(readHeader(encoded)).toEqual({
      key: 'header-key-0',
      value: Buffer.from('header-value-0'),
    });
  });

  it('encodes a null value', () => {
    const encoded = encodeHeader({ key: 'header-key-0', value: null }).buffer;
    expect(readHeader(encoded)).toEqual({ key: 'header-key-0', value: null });
  });
});
