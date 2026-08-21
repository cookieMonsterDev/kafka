import { describe, expect, expectTypeOf, it } from 'vitest';
import { Decoder } from './decoder';
import { Encoder } from './encoder';
import {
  array,
  boolean,
  compactArray,
  compactBytes,
  compactNullableArray,
  compactNullableBytes,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  float64,
  nullableFlexibleObject,
  int16,
  int32,
  int64,
  nullableArray,
  nullableString,
  nullableStruct,
  object,
  string,
  taggedFields,
  uuid,
} from './schema';

describe('protocol/schema', () => {
  it('round-trips a flat object', () => {
    const shape = object([field('a', int16), field('b', boolean), field('c', string)]);
    const encoder = new Encoder();
    shape.write(encoder, { a: 7, b: true, c: 'hello' });

    expectTypeOf<ReturnType<typeof shape.read>>().toEqualTypeOf<{ a: number; b: boolean; c: string }>();

    const decoded = shape.read(new Decoder(encoder.buffer));
    expect(decoded).toEqual({ a: 7, b: true, c: 'hello' });
  });

  it('round-trips nested objects and arrays of objects, in field order', () => {
    const partition = object([field('partitionId', int32), field('leader', int32)]);
    const topic = object([
      field('topic', string),
      field('rack', nullableString),
      field('partitions', array(partition)),
    ]);

    const value = {
      topic: 'my-topic',
      rack: null,
      partitions: [
        { partitionId: 0, leader: 1 },
        { partitionId: 1, leader: 2 },
      ],
    };

    const encoder = new Encoder();
    topic.write(encoder, value);
    const decoded = topic.read(new Decoder(encoder.buffer));
    expect(decoded).toEqual(value);
  });

  it('round-trips an empty array and a bigint field', () => {
    const shape = object([field('offsets', array(int64)), field('count', int32)]);
    const encoder = new Encoder();
    shape.write(encoder, { offsets: [], count: 0 });
    expect(shape.read(new Decoder(encoder.buffer))).toEqual({ offsets: [], count: 0 });

    const withValues = { offsets: [1n, -2n, 9007199254740993n], count: 3 };
    const encoder2 = new Encoder();
    shape.write(encoder2, withValues);
    expect(shape.read(new Decoder(encoder2.buffer))).toEqual(withValues);
  });

  it('nullableArray writes an empty input array as wire length -1', () => {
    const empty = new Encoder();
    nullableArray(string).write(empty, []);
    expect(empty.buffer).toEqual(new Encoder().writeInt32(-1).buffer);
    expect(nullableArray(string).read(new Decoder(empty.buffer))).toEqual([]);

    const withValues = new Encoder();
    nullableArray(string).write(withValues, ['a', 'b']);
    expect(nullableArray(string).read(new Decoder(withValues.buffer))).toEqual(['a', 'b']);
  });

  it('throws when a non-nullable string field is null on the wire', () => {
    const encoder = new Encoder().writeString(null);
    expect(() => string.read(new Decoder(encoder.buffer))).toThrow(RangeError);
  });

  it('round-trips compact strings, including empty and null', () => {
    const hello = new Encoder();
    compactString.write(hello, 'hello');
    expect(compactString.read(new Decoder(hello.buffer))).toBe('hello');

    const empty = new Encoder();
    compactString.write(empty, '');
    expect(compactString.read(new Decoder(empty.buffer))).toBe('');

    const nullable = new Encoder();
    compactNullableString.write(nullable, null);
    expect(compactNullableString.read(new Decoder(nullable.buffer))).toBeNull();
    expect(nullable.buffer).toEqual(new Encoder().writeUVarInt(0).buffer);

    expect(() => compactString.read(new Decoder(new Encoder().writeUVarIntString(null).buffer))).toThrow(RangeError);
  });

  it('round-trips compact bytes, including empty and null', () => {
    const payload = Buffer.from([1, 2, 3]);
    const encoded = new Encoder();
    compactBytes.write(encoded, payload);
    encoded.writeInt32(42);
    const decoder = new Decoder(encoded.buffer);
    expect(compactBytes.read(decoder)).toEqual(payload);
    expect(decoder.readInt32()).toBe(42);

    const empty = new Encoder();
    compactBytes.write(empty, Buffer.alloc(0));
    expect(compactBytes.read(new Decoder(empty.buffer))).toEqual(Buffer.alloc(0));

    const nullable = new Encoder();
    compactNullableBytes.write(nullable, null);
    expect(compactNullableBytes.read(new Decoder(nullable.buffer))).toBeNull();

    expect(() => compactBytes.read(new Decoder(new Encoder().writeUVarIntBytes(null).buffer))).toThrow(RangeError);
  });

  it('round-trips compact arrays and maps a null compact array to []', () => {
    const withValues = new Encoder();
    compactArray(int32).write(withValues, [7, 8]);
    expect(compactArray(int32).read(new Decoder(withValues.buffer))).toEqual([7, 8]);

    const empty = new Encoder();
    compactArray(compactString).write(empty, []);
    expect(empty.buffer).toEqual(new Encoder().writeUVarInt(1).buffer);
    expect(compactArray(compactString).read(new Decoder(empty.buffer))).toEqual([]);

    const nullOnWire = new Encoder().writeUVarInt(0);
    expect(compactArray(int32).read(new Decoder(nullOnWire.buffer))).toEqual([]);
  });

  it('compactNullableArray preserves wire null', () => {
    const nullable = new Encoder();
    compactNullableArray(int32).write(nullable, null);
    expect(nullable.buffer).toEqual(new Encoder().writeUVarInt(0).buffer);
    expect(compactNullableArray(int32).read(new Decoder(nullable.buffer))).toBeNull();

    const empty = new Encoder();
    compactNullableArray(int32).write(empty, []);
    expect(compactNullableArray(int32).read(new Decoder(empty.buffer))).toEqual([]);
  });

  it('round-trips float64 (IEEE 754 binary64)', () => {
    for (const value of [0, -1, 1048576, 50.5]) {
      const encoded = new Encoder();
      float64.write(encoded, value);
      expect(encoded.buffer).toEqual(new Encoder().writeDouble(value).buffer);
      expect(float64.read(new Decoder(encoded.buffer))).toBe(value);
    }
  });

  it('nullableStruct encodes null as INT8 -1 and present as INT8 1 plus the body', () => {
    const shape = nullableStruct(flexibleObject([field('count', int32)]));
    const nullEncoder = new Encoder();
    shape.write(nullEncoder, null);
    expect(nullEncoder.buffer).toEqual(new Encoder().writeInt8(-1).buffer);
    expect(shape.read(new Decoder(nullEncoder.buffer))).toBeNull();

    const present = new Encoder();
    shape.write(present, { count: 3 });
    expect(present.buffer).toEqual(new Encoder().writeInt8(1).writeInt32(3).writeUVarInt(0).buffer);
    expect(shape.read(new Decoder(present.buffer))).toEqual({ count: 3 });
  });

  it('round-trips an empty tagged-fields buffer', () => {
    const encoder = new Encoder();
    taggedFields.write(encoder, null);
    expect(encoder.buffer).toEqual(Buffer.from([0]));
    expect(taggedFields.read(new Decoder(encoder.buffer))).toBeNull();
  });

  it('flexibleObject appends and skips a trailing TAG_BUFFER', () => {
    const shape = flexibleObject([field('name', compactString), field('count', int32)]);
    const encoder = new Encoder();
    shape.write(encoder, { name: 'topic', count: 2 });

    const expected = new Encoder().writeUVarIntString('topic').writeInt32(2).writeUVarInt(0);
    expect(encoder.buffer).toEqual(expected.buffer);
    expect(shape.read(new Decoder(encoder.buffer))).toEqual({ name: 'topic', count: 2 });
  });

  it('round-trips a nullable flexible struct, including null', () => {
    const cursor = nullableFlexibleObject([field('topic', compactString), field('partitionIndex', int32)]);
    const present = { topic: 'orders', partitionIndex: 3 };

    const encoded = new Encoder();
    cursor.write(encoded, present);
    expect(encoded.buffer).toEqual(
      new Encoder().writeInt8(1).writeUVarIntString('orders').writeInt32(3).writeUVarInt(0).buffer,
    );
    expect(cursor.read(new Decoder(encoded.buffer))).toEqual(present);

    const nullable = new Encoder();
    cursor.write(nullable, null);
    expect(nullable.buffer).toEqual(new Encoder().writeInt8(-1).buffer);
    expect(cursor.read(new Decoder(nullable.buffer))).toBeNull();
  });

  it('round-trips a 16-byte UUID', () => {
    const id = Buffer.from('0123456789abcdef');
    const encoded = new Encoder();
    uuid.write(encoded, id);
    expect(uuid.read(new Decoder(encoded.buffer))).toEqual(id);
    expect(() => uuid.write(new Encoder(), Buffer.from([1, 2, 3]))).toThrow(RangeError);
  });
});
