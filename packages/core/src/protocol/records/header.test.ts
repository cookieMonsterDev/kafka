import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder.js';
import { decodeHeader, encodeHeader } from './header.js';

describe('protocol/records/header', () => {
  it('round-trips a key/value pair', () => {
    const encoded = encodeHeader({ key: 'header-key-0', value: Buffer.from('header-value-0') }).buffer;
    expect(decodeHeader(new Decoder(encoded))).toEqual({
      key: 'header-key-0',
      value: Buffer.from('header-value-0'),
    });
  });

  it('round-trips a null value', () => {
    const encoded = encodeHeader({ key: 'header-key-0', value: null }).buffer;
    expect(decodeHeader(new Decoder(encoded))).toEqual({ key: 'header-key-0', value: null });
  });
});
