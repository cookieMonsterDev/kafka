import { describe, expect, it } from 'vitest';
import { KafkaInvalidLongError, KafkaInvalidVarIntError } from '../errors';
import { Decoder } from './decoder';
import { Encoder } from './encoder';

const MAX_INT64 = 9223372036854775807n;
const MIN_INT64 = -9223372036854775808n;

/** The BigInt-only varlong decoding that `readVarLong` used before its `number` fast path. */
function referenceReadVarLong(buffer: Buffer): { value: bigint; bytesRead: number } {
  let result = 0n;
  let shift = 0n;
  let bytesRead = 0;
  let currentByte: number;
  do {
    currentByte = buffer[bytesRead] ?? 0;
    bytesRead += 1;
    result += BigInt(currentByte & 0x7f) << shift;
    shift += 7n;
  } while (currentByte >= 0x80);
  const value = (result >> 1n) ^ ((result & 1n) === 0n ? 0n : -1n);
  return { value, bytesRead };
}

// Seven varlong bytes hold 49 zigzag bits: [-2^48, 2^48 - 1] is the last range the fast path covers.
const LAST_SEVEN_BYTE_POSITIVE = 2n ** 48n - 1n;
const LAST_SEVEN_BYTE_NEGATIVE = -(2n ** 48n);
const VARLONG_BOUNDARY_VALUES = [
  0n,
  1n,
  -1n,
  63n,
  -64n,
  64n,
  -65n,
  LAST_SEVEN_BYTE_POSITIVE,
  LAST_SEVEN_BYTE_NEGATIVE,
  LAST_SEVEN_BYTE_POSITIVE + 1n,
  LAST_SEVEN_BYTE_NEGATIVE - 1n,
  BigInt(Number.MAX_SAFE_INTEGER),
  BigInt(Number.MIN_SAFE_INTEGER),
  2n ** 53n,
  -(2n ** 53n),
  2n ** 53n + 1n,
  -(2n ** 53n) - 1n,
  MAX_INT64,
  MIN_INT64,
];

describe('protocol/Decoder', () => {
  describe('readInt64', () => {
    it('round-trips values at the ±2^63 boundary', () => {
      for (const n of [MAX_INT64, MIN_INT64, 0n, -1n, 1n, 2n ** 62n, -(2n ** 62n)]) {
        const buffer = new Encoder().writeInt64(n).buffer;
        expect(new Decoder(buffer).readInt64()).toEqual(n);
      }
    });

    it('accepts a plain number for convenience (e.g. a Date.now() timestamp)', () => {
      const now = Date.now();
      const buffer = new Encoder().writeInt64(now).buffer;
      expect(new Decoder(buffer).readInt64()).toEqual(BigInt(now));
    });
  });

  describe('readInt8/readInt16/readInt32', () => {
    it('round-trips signed values, including negative ones', () => {
      expect(new Decoder(new Encoder().writeInt8(-1).buffer).readInt8()).toEqual(-1);
      expect(new Decoder(new Encoder().writeInt16(-1).buffer).readInt16()).toEqual(-1);
      expect(new Decoder(new Encoder().writeUInt16(65535).buffer).readUInt16()).toEqual(65535);
      expect(new Decoder(new Encoder().writeInt32(-1).buffer).readInt32()).toEqual(-1);
    });
  });

  describe('readString / readBytes (length-prefixed)', () => {
    it('round-trips a string', () => {
      const buffer = new Encoder().writeString('kafka').buffer;
      expect(new Decoder(buffer).readString()).toEqual('kafka');
    });

    it('decodes a null string as null (length -1)', () => {
      const buffer = new Encoder().writeString(null).buffer;
      expect(new Decoder(buffer).readString()).toBeNull();
    });

    it('round-trips bytes', () => {
      const buffer = new Encoder().writeBytes(Buffer.from([1, 2, 3])).buffer;
      expect(new Decoder(buffer).readBytes()).toEqual(Buffer.from([1, 2, 3]));
    });

    it('decodes null bytes as null (length -1)', () => {
      const buffer = new Encoder().writeBytes(null).buffer;
      expect(new Decoder(buffer).readBytes()).toBeNull();
    });
  });

  describe('readVarIntString / readVarIntBytes', () => {
    it('round-trips a string', () => {
      const buffer = new Encoder().writeVarIntString('kafka').buffer;
      expect(new Decoder(buffer).readVarIntString()).toEqual('kafka');
    });

    it('decodes a null varint string as null', () => {
      const buffer = new Encoder().writeVarIntString(null).buffer;
      expect(new Decoder(buffer).readVarIntString()).toBeNull();
    });

    it('round-trips bytes', () => {
      const buffer = new Encoder().writeVarIntBytes(Buffer.from([9, 8, 7])).buffer;
      expect(new Decoder(buffer).readVarIntBytes()).toEqual(Buffer.from([9, 8, 7]));
    });
  });

  describe('readUVarIntBytes', () => {
    it('round-trips compact bytes that are followed by more fields', () => {
      const buffer = new Encoder().writeUVarIntBytes(Buffer.from([1, 2, 3])).writeInt32(42).buffer;
      const decoder = new Decoder(buffer);
      expect(decoder.readUVarIntBytes()).toEqual(Buffer.from([1, 2, 3]));
      expect(decoder.readInt32()).toBe(42);
    });

    it('round-trips empty compact bytes that are followed by more fields', () => {
      const buffer = new Encoder().writeUVarIntBytes('').writeInt32(42).buffer;
      const decoder = new Decoder(buffer);
      expect(decoder.readUVarIntBytes()).toEqual(Buffer.alloc(0));
      expect(decoder.readInt32()).toBe(42);
    });
  });

  describe('readArray / readVarIntArray', () => {
    it('round-trips a fixed-length-prefixed array', () => {
      const buffer = new Encoder().writeArray([1, 2, 3], 'int32').buffer;
      expect(new Decoder(buffer).readArray((d) => d.readInt32())).toEqual([1, 2, 3]);
    });

    it('decodes a -1 length as an empty array', () => {
      const buffer = new Encoder().writeInt32(-1).buffer;
      expect(new Decoder(buffer).readArray((d) => d.readInt32())).toEqual([]);
    });
  });

  describe('readTaggedFields', () => {
    it('returns null when there are no tagged fields', () => {
      const buffer = new Encoder().writeUVarInt(0).buffer;
      expect(new Decoder(buffer).readTaggedFields()).toBeNull();
    });

    it('skips over present tagged fields without throwing', () => {
      const buffer = new Encoder()
        .writeUVarInt(1) // one tagged field
        .writeUVarInt(5) // tag
        .writeUVarInt(3) // raw size (KIP-482), not compact N+1
        .writeBuffer(Buffer.from([1, 2, 3])).buffer;

      const decoder = new Decoder(buffer);
      expect(decoder.readTaggedFields()).toEqual({});
      expect(decoder.offset).toBe(buffer.length);
    });
  });

  describe('forEachTaggedField', () => {
    it('visits each tag with a decoder scoped to its raw-sized value', () => {
      const buffer = new Encoder()
        .writeUVarInt(2)
        .writeUVarInt(0)
        .writeUVarInt(4)
        .writeInt32(42)
        .writeUVarInt(3)
        .writeUVarInt(1)
        .writeInt8(7)
        .writeInt8(9).buffer;

      const decoder = new Decoder(buffer);
      const seen: [number, number][] = [];
      const count = decoder.forEachTaggedField((tag, field) => {
        seen.push([tag, tag === 0 ? field.readInt32() : field.readInt8()]);
      });

      expect(count).toBe(2);
      expect(seen).toEqual([
        [0, 42],
        [3, 7],
      ]);
      expect(decoder.readInt8()).toBe(9);
    });
  });

  describe('canReadBytes / canReadInt16 / canReadInt32 / canReadInt64', () => {
    it('reports whether enough bytes remain', () => {
      const decoder = new Decoder(Buffer.alloc(4));
      expect(decoder.canReadInt32()).toBe(true);
      expect(decoder.canReadInt64()).toBe(false);
      decoder.forward(4);
      expect(decoder.canReadInt16()).toBe(false);
    });

    it('uses the buffer length remaining after the current offset', () => {
      const decoder = new Decoder(Buffer.alloc(8));
      decoder.forward(5);
      expect(decoder.canReadBytes(3)).toBe(true);
      expect(decoder.canReadBytes(4)).toBe(false);
    });
  });

  describe('slice / forward', () => {
    it('slice returns an independent decoder scoped to the given size', () => {
      const buffer = new Encoder().writeInt32(1).writeInt32(2).buffer;
      const outer = new Decoder(buffer);
      const inner = outer.slice(4);
      expect(inner.readInt32()).toEqual(1);
      expect(outer.offset).toBe(0);
      outer.forward(4);
      expect(outer.readInt32()).toEqual(2);
    });
  });

  describe('readVarLong / readVarLongNumberOrBigInt', () => {
    it('decodes boundary values exactly like the BigInt-only decoder', () => {
      for (const value of VARLONG_BOUNDARY_VALUES) {
        const buffer = new Encoder().writeVarLong(value).buffer;
        const reference = referenceReadVarLong(buffer);
        const decoder = new Decoder(buffer);

        expect(reference.value).toBe(value);
        expect(decoder.readVarLong()).toBe(value);
        expect(decoder.offset).toBe(reference.bytesRead);
      }
    });

    it('returns a number up to 7 encoded bytes and a bigint from 8 bytes on', () => {
      for (const value of VARLONG_BOUNDARY_VALUES) {
        const buffer = new Encoder().writeVarLong(value).buffer;
        const decoded = new Decoder(buffer).readVarLongNumberOrBigInt();

        expect(typeof decoded).toBe(buffer.length <= 7 ? 'number' : 'bigint');
        expect(BigInt(decoded)).toBe(value);
      }
      expect(new Encoder().writeVarLong(LAST_SEVEN_BYTE_POSITIVE).buffer).toHaveLength(7);
      expect(new Encoder().writeVarLong(LAST_SEVEN_BYTE_NEGATIVE).buffer).toHaveLength(7);
      expect(new Encoder().writeVarLong(LAST_SEVEN_BYTE_POSITIVE + 1n).buffer).toHaveLength(8);
      expect(new Encoder().writeVarLong(LAST_SEVEN_BYTE_NEGATIVE - 1n).buffer).toHaveLength(8);
    });

    it('decodes non-canonical, zero-padded encodings on both sides of the 7-byte boundary', () => {
      const paddedToSevenBytes = Buffer.from([0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
      const paddedToEightBytes = Buffer.from([0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);

      for (const buffer of [paddedToSevenBytes, paddedToEightBytes]) {
        const decoder = new Decoder(buffer);
        expect(decoder.readVarLong()).toBe(referenceReadVarLong(buffer).value);
        expect(decoder.offset).toBe(buffer.length);
      }
    });

    it('matches the BigInt-only decoder across random 64-bit values', () => {
      let seed = 0x9e3779b97f4a7c15n;
      for (let i = 0; i < 500; i++) {
        seed = BigInt.asUintN(64, seed * 6364136223846793005n + 1442695040888963407n);
        const value = BigInt.asIntN(64, seed >> BigInt(i % 64));
        const buffer = new Encoder().writeVarLong(value).buffer;

        expect(new Decoder(buffer).readVarLong()).toBe(referenceReadVarLong(buffer).value);
      }
    });

    it('reads consecutive varlongs without losing its position', () => {
      const encoder = new Encoder();
      for (const value of VARLONG_BOUNDARY_VALUES) encoder.writeVarLong(value);
      const decoder = new Decoder(encoder.buffer);

      for (const value of VARLONG_BOUNDARY_VALUES) {
        expect(decoder.readVarLong()).toBe(value);
      }
      expect(decoder.offset).toBe(encoder.buffer.length);
    });
  });

  describe('negative lengths', () => {
    it('rejects a length below -1 instead of moving the offset backwards', () => {
      const varIntPrefixed = new Encoder().writeVarInt(-5).writeBuffer(Buffer.alloc(8)).buffer;
      const int16Prefixed = new Encoder().writeInt16(-5).writeBuffer(Buffer.alloc(8)).buffer;
      const int32Prefixed = new Encoder().writeInt32(-5).writeBuffer(Buffer.alloc(8)).buffer;

      expect(() => new Decoder(varIntPrefixed).readVarIntBytes()).toThrow(RangeError);
      expect(() => new Decoder(varIntPrefixed).readVarIntString()).toThrow(RangeError);
      expect(() => new Decoder(int16Prefixed).readString()).toThrow(RangeError);
      expect(() => new Decoder(int32Prefixed).readBytes()).toThrow(RangeError);
      expect(() => new Decoder(Buffer.alloc(8)).readBytes(-2)).toThrow(RangeError);
    });

    it('still decodes a -1 length as null', () => {
      expect(new Decoder(new Encoder().writeVarInt(-1).buffer).readVarIntBytes()).toBeNull();
      expect(new Decoder(new Encoder().writeVarInt(-1).buffer).readVarIntString()).toBeNull();
      expect(new Decoder(new Encoder().writeInt16(-1).buffer).readString()).toBeNull();
      expect(new Decoder(new Encoder().writeInt32(-1).buffer).readBytes()).toBeNull();
    });
  });

  describe('truncated-buffer errors', () => {
    it('throws a RangeError when readVarInt runs past the end of the buffer', () => {
      // Continuation bit set, no following byte.
      expect(() => new Decoder(Buffer.from([0x80])).readVarInt()).toThrow(RangeError);
    });

    it('throws KafkaInvalidVarIntError when a signed varint never terminates within 5 bytes', () => {
      expect(() => new Decoder(Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff, 0x01])).readVarInt()).toThrow(
        KafkaInvalidVarIntError,
      );
    });

    it('throws KafkaInvalidLongError when a signed varlong never terminates within 9 bytes', () => {
      expect(() => new Decoder(Buffer.alloc(11, 0xff)).readVarLong()).toThrow(KafkaInvalidLongError);
    });

    it('throws a RangeError when readUVarInt runs past the end of the buffer', () => {
      expect(() => new Decoder(Buffer.from([0x80])).readUVarInt()).toThrow(RangeError);
    });

    it('throws a RangeError when readVarLong runs past the end of the buffer', () => {
      expect(() => new Decoder(Buffer.from([0x80])).readVarLong()).toThrow(RangeError);
    });

    it('throws when reading a fixed-width field past the end of the buffer', () => {
      expect(() => new Decoder(Buffer.alloc(0)).readInt32()).toThrow();
      expect(() => new Decoder(Buffer.alloc(0)).readInt64()).toThrow();
    });
  });
});
