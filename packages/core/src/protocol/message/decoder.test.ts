import { describe, expect, it } from 'vitest';
import { KafkaCorruptRecordError, KafkaPartialMessageError, KafkaUnsupportedMagicByteInMessageSet } from '../../errors';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { COMPRESSION_TYPES } from '../compression/index';
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

  it('encodes the compression codec in the attributes byte', () => {
    const encoded = encodeMessageV0({ compression: COMPRESSION_TYPES.GZIP, key: null, value: Buffer.from('x') });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    decoder.readInt8();
    expect(decodeMessageV0(decoder).attributes).toBe(COMPRESSION_TYPES.GZIP);
  });

  it('masks compression attributes to the codec bits', () => {
    const encoded = encodeMessageV0({ compression: (COMPRESSION_TYPES.LZ4 | 0x10) as never, value: 'x' });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    decoder.readInt8();
    expect(decodeMessageV0(decoder).attributes).toBe(COMPRESSION_TYPES.LZ4);
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

  it('defaults timestamp to Date.now() when omitted', () => {
    const before = Date.now();
    const encoded = encodeMessageV1({ key: null, value: 'v' });
    const after = Date.now();
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    decoder.readInt8();
    const decoded = decodeMessageV1(decoder);
    expect(decoded.timestamp).toBeGreaterThanOrEqual(BigInt(before));
    expect(decoded.timestamp).toBeLessThanOrEqual(BigInt(after));
  });

  it('encodes the compression codec in the attributes byte', () => {
    const encoded = encodeMessageV1({
      compression: COMPRESSION_TYPES.Snappy,
      timestamp: 1,
      key: null,
      value: 'v',
    });
    const decoder = new Decoder(encoded.buffer);
    decoder.readInt32();
    decoder.readInt8();
    expect(decodeMessageV1(decoder).attributes).toBe(COMPRESSION_TYPES.Snappy);
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

  describe('checkCrcs', () => {
    it('defaults to true and rejects a message whose CRC does not match its bytes', () => {
      const encoded = encodeMessageV0({ key: Buffer.from('k'), value: Buffer.from('hello') });
      const corrupted = Buffer.from(encoded.buffer);
      // Flip a byte inside the value, well after the CRC field.
      corrupted[corrupted.length - 1] = (corrupted[corrupted.length - 1] as number) ^ 0xff;

      const decoder = new Decoder(corrupted);
      expect(() => decodeMessage(0n, corrupted.length, decoder)).toThrow(KafkaCorruptRecordError);
    });

    it('checkCrcs: false skips the check and decodes the corrupted message anyway', () => {
      const encoded = encodeMessageV0({ key: Buffer.from('k'), value: Buffer.from('hello') });
      const corrupted = Buffer.from(encoded.buffer);
      corrupted[corrupted.length - 1] = (corrupted[corrupted.length - 1] as number) ^ 0xff;

      const decoder = new Decoder(corrupted);
      const decoded = decodeMessage(0n, corrupted.length, decoder, false);
      expect(decoded.value).not.toEqual(Buffer.from('hello'));
    });
  });
});
