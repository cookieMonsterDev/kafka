import { describe, expect, it } from 'vitest';
import { Decoder } from './decoder';
import { Encoder } from './encoder';

const MAX_SAFE_POSITIVE_SIGNED_INT = 2147483647;
const MIN_SAFE_NEGATIVE_SIGNED_INT = -2147483648;

const MAX_SAFE_UNSIGNED_INT = 4294967295;
const MIN_SAFE_UNSIGNED_INT = 0;

const MAX_INT64 = 9223372036854775807n;
const MIN_INT64 = -9223372036854775808n;

const signed32 = (n: number) => new Encoder().writeVarInt(n).buffer;
const unsigned32 = (n: number) => new Encoder().writeUVarInt(n).buffer;
const signed64 = (n: bigint) => new Encoder().writeVarLong(n).buffer;
const encodeDouble = (n: number) => new Encoder().writeDouble(n).buffer;
const ustring = (s: string | null) => new Encoder().writeUVarIntString(s).buffer;
const ubytes = (b: string | null) => new Encoder().writeUVarIntBytes(b).buffer;
const uarray = (array: readonly Encoder[]) => new Encoder().writeUVarIntArray(array).buffer;

const decode32 = (buffer: Buffer) => new Decoder(buffer).readVarInt();
const decode32u = (buffer: Buffer) => new Decoder(buffer).readUVarInt();
const decode64 = (buffer: Buffer) => new Decoder(buffer).readVarLong();
const decodeDouble = (buffer: Buffer) => new Decoder(buffer).readDouble();
const decodeUString = (buffer: Buffer) => new Decoder(buffer).readUVarIntString();
const decodeUBytes = (buffer: Buffer) => new Decoder(buffer).readUVarIntBytes();

const B = (...args: number[]) => Buffer.from(args);

describe('protocol/Encoder', () => {
  describe('unsigned varint array', () => {
    const array = [7681, 823, 9123, 9812, 3219];

    it('encodes', () => {
      const encoded = array.map((n) => new Encoder().writeUVarInt(n));
      expect(uarray(encoded)).toEqual(B(0x06, 0x81, 0x3c, 0xb7, 0x06, 0xa3, 0x47, 0xd4, 0x4c, 0x93, 0x19));
    });

    it('decodes', () => {
      const encoded = array.map((n) => new Encoder().writeUVarInt(n));
      const decoder = new Decoder(uarray(encoded));
      expect(decoder.readUVarIntArray((d) => d.readUVarInt())).toEqual(array);
    });
  });

  describe('unsigned varint bytes', () => {
    it('encodes', () => {
      expect(ubytes(null)).toEqual(B(0x00));
      expect(ubytes('')).toEqual(B(0x01));
      expect(ubytes('kafka')).toEqual(B(0x06, 0x6b, 0x61, 0x66, 0x6b, 0x61));
    });

    it('decodes', () => {
      expect(decodeUBytes(ubytes(null))).toEqual(null);
      expect(decodeUBytes(ubytes(''))).toEqual(B());
      expect(decodeUBytes(ubytes('kafka'))).toEqual(B(0x6b, 0x61, 0x66, 0x6b, 0x61));
    });
  });

  describe('unsigned varint string', () => {
    it('encodes', () => {
      expect(ustring(null)).toEqual(B(0x00));
      expect(ustring('')).toEqual(B(0x01));
      expect(ustring('kafka')).toEqual(B(0x06, 0x6b, 0x61, 0x66, 0x6b, 0x61));
    });

    it('decodes', () => {
      expect(decodeUString(ustring(null))).toEqual(null);
      expect(decodeUString(ustring(''))).toEqual('');
      expect(decodeUString(ustring('kafka'))).toEqual('kafka');
    });
  });

  describe('writeEncoder', () => {
    it('appends the value buffer to the existing encoder', () => {
      const encoder = new Encoder().writeBuffer(B(1)).writeEncoder(new Encoder().writeBuffer(B(2)));
      expect(encoder.buffer).toEqual(B(1, 2));
    });
  });

  describe('writeEncoderArray', () => {
    it('appends all encoder values to the existing encoder', () => {
      const values = [
        new Encoder().writeBuffer(B(1)),
        new Encoder().writeBuffer(B(2)),
        new Encoder().writeBuffer(B(3)),
      ];
      expect(new Encoder().writeEncoderArray(values).buffer).toEqual(B(1, 2, 3));
    });
  });

  describe('double', () => {
    it('encodes', () => {
      expect(encodeDouble(-3.141592653589793)).toEqual(B(0xc0, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18));
      expect(encodeDouble(-0.3333333333333333)).toEqual(B(0xbf, 0xd5, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55));
      expect(encodeDouble(-1.5)).toEqual(B(0xbf, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00));
      expect(encodeDouble(0.0)).toEqual(B(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00));
      expect(encodeDouble(1.5)).toEqual(B(0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00));
      expect(encodeDouble(3.141592653589793)).toEqual(B(0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18));
    });

    it('round-trips', () => {
      for (const n of [-3.141592653589793, -0.3333333333333333, -1.5, 0.0, 1.5, 3.141592653589793]) {
        expect(decodeDouble(encodeDouble(n))).toEqual(n);
      }
    });
  });

  describe('varint', () => {
    it('encodes signed int32 numbers', () => {
      expect(signed32(0)).toEqual(B(0x00));
      expect(signed32(1)).toEqual(B(0x02));
      expect(signed32(63)).toEqual(B(0x7e));
      expect(signed32(64)).toEqual(B(0x80, 0x01));
      expect(signed32(8191)).toEqual(B(0xfe, 0x7f));
      expect(signed32(8192)).toEqual(B(0x80, 0x80, 0x01));
      expect(signed32(-1)).toEqual(B(0x01));
      expect(signed32(-64)).toEqual(B(0x7f));
      expect(signed32(-65)).toEqual(B(0x81, 0x01));
    });

    it('encodes signed int32 boundaries', () => {
      expect(signed32(MAX_SAFE_POSITIVE_SIGNED_INT)).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0x0f));
      expect(signed32(MIN_SAFE_NEGATIVE_SIGNED_INT)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x0f));
    });

    it('round-trips signed int32 numbers, including boundaries', () => {
      const values = [
        0,
        1,
        63,
        64,
        8191,
        8192,
        1048575,
        1048576,
        134217727,
        134217728,
        -1,
        -64,
        -65,
        -8192,
        -8193,
        -1048576,
        -1048577,
        -134217728,
        -134217729,
        MAX_SAFE_POSITIVE_SIGNED_INT,
        MIN_SAFE_NEGATIVE_SIGNED_INT,
      ];
      for (const n of values) {
        expect(decode32(signed32(n))).toEqual(n);
      }
    });
  });

  describe('uvarint', () => {
    it('encodes unsigned int32 numbers', () => {
      expect(unsigned32(0)).toEqual(B(0x00));
      expect(unsigned32(1)).toEqual(B(0x01));
      expect(unsigned32(127)).toEqual(B(0x7f));
      expect(unsigned32(128)).toEqual(B(0x80, 0x01));
      expect(unsigned32(16383)).toEqual(B(0xff, 0x7f));
      expect(unsigned32(16384)).toEqual(B(0x80, 0x80, 0x01));
    });

    it('encodes unsigned int32 boundaries', () => {
      expect(unsigned32(MAX_SAFE_UNSIGNED_INT)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0x0f));
      expect(unsigned32(MIN_SAFE_UNSIGNED_INT)).toEqual(B(0x00));
    });

    it('round-trips unsigned int32 numbers, including boundaries', () => {
      const values = [
        0,
        1,
        127,
        128,
        8192,
        16383,
        16384,
        2097151,
        134217728,
        268435455,
        MAX_SAFE_UNSIGNED_INT,
        MIN_SAFE_UNSIGNED_INT,
      ];
      for (const n of values) {
        expect(decode32u(unsigned32(n))).toEqual(n);
      }
    });

    it('throws on a malformed (too long) uvarint', () => {
      expect(() => decode32u(B(0xff, 0xff, 0xff, 0xff, 0xff, 0x01))).toThrow();
    });
  });

  describe('varlong', () => {
    it('encodes signed int64 numbers', () => {
      expect(signed64(0n)).toEqual(B(0x00));
      expect(signed64(1n)).toEqual(B(0x02));
      expect(signed64(64n)).toEqual(B(0x80, 0x01));
      expect(signed64(17179869183n)).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0x7f));
      expect(signed64(17179869184n)).toEqual(B(0x80, 0x80, 0x80, 0x80, 0x80, 0x01));
      expect(signed64(MAX_INT64)).toEqual(B(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01));

      expect(signed64(-1n)).toEqual(B(0x01));
      expect(signed64(-64n)).toEqual(B(0x7f));
      expect(signed64(-65n)).toEqual(B(0x81, 0x01));
      expect(signed64(MIN_INT64)).toEqual(B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01));
    });

    it('round-trips signed int64 numbers, including 64-bit boundaries', () => {
      const values = [
        0n,
        1n,
        63n,
        64n,
        8191n,
        8192n,
        17179869183n,
        17179869184n,
        2199023255551n,
        2199023255552n,
        281474976710655n,
        281474976710656n,
        -1n,
        -64n,
        -65n,
        -17179869184n,
        -17179869185n,
        MAX_INT64,
        MIN_INT64,
      ];
      for (const n of values) {
        expect(decode64(signed64(n))).toEqual(n);
      }
    });

    it('throws when decoding a malformed (too long) varlong', () => {
      expect(() => decode64(B(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01))).toThrow();
    });
  });

  describe('sizeOfVarInt', () => {
    it('matches the actual encoded length', () => {
      for (const n of [
        0,
        1,
        63,
        64,
        8191,
        8192,
        -1,
        -64,
        -65,
        MAX_SAFE_POSITIVE_SIGNED_INT,
        MIN_SAFE_NEGATIVE_SIGNED_INT,
      ]) {
        expect(Encoder.sizeOfVarInt(n)).toEqual(signed32(n).length);
      }
    });
  });

  describe('sizeOfVarLong', () => {
    it('matches the actual encoded length', () => {
      for (const n of [0n, 1n, 63n, 64n, 17179869183n, 17179869184n, MIN_INT64, MAX_INT64]) {
        expect(Encoder.sizeOfVarLong(n)).toEqual(signed64(n).length);
      }
    });
  });

  describe('resizing', () => {
    it('copies existing content when resizing', () => {
      const encoder = new Encoder(4);
      encoder.writeBuffer(B(1, 2, 3, 4));
      encoder.writeBuffer(B(5, 6, 7, 8));
      expect(encoder.buffer).toEqual(B(1, 2, 3, 4, 5, 6, 7, 8));
    });

    it('obeys offset when resizing', () => {
      const encoder = new Encoder(4);
      encoder.writeBuffer(B(1, 2)); // only two bytes in, ...
      encoder.writeBuffer(B(5, 6, 7, 8)); // ... but this write requires resizing
      expect(encoder.buffer).toEqual(B(1, 2, 5, 6, 7, 8));
    });
  });

  describe('inline varints', () => {
    it('writes sequential uvarints into one buffer without per-field copies', () => {
      const values = [0, 1, 127, 128, 16384, MAX_SAFE_UNSIGNED_INT];
      const encoder = new Encoder();
      for (const n of values) encoder.writeUVarInt(n);
      expect(encoder.buffer).toEqual(Buffer.concat(values.map((n) => unsigned32(n))));
    });

    it('writes sequential varlongs into one buffer without per-field copies', () => {
      const values = [0n, 1n, -1n, 64n, MIN_INT64, MAX_INT64];
      const encoder = new Encoder();
      for (const n of values) encoder.writeVarLong(n);
      expect(encoder.buffer).toEqual(Buffer.concat(values.map((n) => signed64(n))));
    });
  });

  describe('writeInt64', () => {
    it('encodes bigint and number with the same bytes for safe integers', () => {
      expect(new Encoder().writeInt64(42n).buffer).toEqual(new Encoder().writeInt64(42).buffer);
      expect(new Encoder().writeInt64(0n).buffer).toEqual(new Encoder().writeInt64(0).buffer);
      expect(new Encoder().writeInt64(-1n).buffer).toEqual(new Encoder().writeInt64(-1).buffer);
    });
  });

  describe('writeInt32At / writeUInt32At', () => {
    it('patches a reserved length prefix without moving the cursor', () => {
      const encoder = new Encoder();
      encoder.writeInt32(0);
      encoder.writeInt16(7);
      encoder.writeInt32At(0, encoder.size() - 4);
      expect(encoder.buffer.readInt32BE(0)).toBe(encoder.size() - 4);
      expect(encoder.buffer.readInt16BE(4)).toBe(7);
    });

    it('patches a reserved unsigned CRC slot', () => {
      const encoder = new Encoder();
      encoder.writeUInt32(0);
      encoder.writeInt8(2);
      encoder.writeUInt32At(0, 0xe3069283);
      expect(encoder.buffer.readUInt32BE(0)).toBe(0xe3069283);
      expect(encoder.buffer.readInt8(4)).toBe(2);
    });
  });

  describe('toJSON', () => {
    it('matches the shape of Buffer#toJSON, for fixture compatibility', () => {
      const encoder = new Encoder().writeBuffer(B(1, 2, 3));
      expect(encoder.toJSON()).toEqual(encoder.buffer.toJSON());
    });
  });
});
