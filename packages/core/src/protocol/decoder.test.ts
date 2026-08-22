import { describe, expect, it } from 'vitest';
import { KafkaInvalidLongError, KafkaInvalidVarIntError } from '../errors';
import { Decoder } from './decoder';
import { Encoder } from './encoder';

const MAX_INT64 = 9223372036854775807n;
const MIN_INT64 = -9223372036854775808n;

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
        .writeUVarIntBytes(
          Buffer.from([1, 2, 3]),
        ) // value
      .buffer;

      const decoder = new Decoder(buffer);
      expect(decoder.readTaggedFields()).toEqual({});
      expect(decoder.offset).toBe(buffer.length);
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
