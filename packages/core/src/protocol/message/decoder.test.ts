import { describe, expect, it } from 'vitest';
import { KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { decodeMessage } from './decoder';
import { decodeMessageV0, encodeMessageV0 } from './v0';
import { decodeMessageV1, encodeMessageV1 } from './v1';

describe('protocol/message/v0', () => {
  it('round-trips key and value', () => {
    const encoded = encodeMessageV0({ key: Buffer.from('k'), value: Buffer.from('v') });
    const decoder = new Decoder(encoded.buffer);
    expect(decoder.readInt32()).toEqual(expect.any(Number));
    expect(decoder.readInt8()).toBe(0);
    expect(decodeMessageV0(decoder)).toEqual({ attributes: 0, key: Buffer.from('k'), value: Buffer.from('v') });
  });

  it('round-trips null key and value', () => {
    const encoded = encodeMessageV0({ key: null, value: null });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    decoder.readInt8();
    expect(decodeMessageV0(decoder)).toEqual({ attributes: 0, key: null, value: null });
  });
});

describe('protocol/message/v1', () => {
  it('round-trips timestamp, key, and value', () => {
    const encoded = encodeMessageV1({ timestamp: 1_700_000_000_000, key: 'k', value: 'v' });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    expect(decoder.readInt8()).toBe(1);
    expect(decodeMessageV1(decoder)).toEqual({
      attributes: 0,
      timestamp: 1_700_000_000_000n,
      key: Buffer.from('k'),
      value: Buffer.from('v'),
    });
  });
});

describe('protocol/message/decoder', () => {
  it('decodes a v0 message including offset, size, crc, and magic', () => {
    const encoded = encodeMessageV0({ key: Buffer.from('k'), value: Buffer.from('hello') });
    const decoder = new Decoder(encoded.buffer);
    const decoded = decodeMessage(42n, encoded.buffer.length, decoder);
    expect(decoded).toMatchObject({
      offset: 42n,
      size: encoded.buffer.length,
      magicByte: 0,
      key: Buffer.from('k'),
      value: Buffer.from('hello'),
    });
    expect(decoded.crc).toEqual(expect.any(Number));
  });

  it('decodes a v1 message', () => {
    const encoded = encodeMessageV1({ timestamp: 99, key: null, value: Buffer.from('x') });
    const decoder = new Decoder(encoded.buffer);
    const decoded = decodeMessage(1n, encoded.buffer.length, decoder);
    expect(decoded.magicByte).toBe(1);
    expect(decoded.timestamp).toBe(99n);
    expect(decoded.value).toEqual(Buffer.from('x'));
  });

  it('throws when the remaining bytes are shorter than the declared size', () => {
    const decoder = new Decoder(Buffer.alloc(4));
    expect(() => decodeMessage(0n, 16, decoder)).toThrow(KafkaPartialMessageError);
  });

  it('throws for an unsupported magic byte', () => {
    const payload = new Encoder().writeInt32(0).writeInt8(2).writeInt8(0).writeBytes(null).writeBytes(null);
    const decoder = new Decoder(payload.buffer);
    expect(() => decodeMessage(0n, payload.buffer.length, decoder)).toThrow(KafkaUnsupportedMagicByteInMessageSet);
  });
});
