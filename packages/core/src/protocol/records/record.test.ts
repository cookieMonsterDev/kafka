import { describe, expect, it } from 'vitest';
import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import { decodeRecord, encodeRecord, type RecordBatchContext } from './record';

// Record with a key, a value, and two headers (one single-valued, one repeated).
const V0_RECORD_FIXTURE = Buffer.from([
  0, 0, 0, 10, 107, 101, 121, 45, 48, 24, 115, 111, 109, 101, 45, 118, 97, 108, 117, 101, 45, 48, 6, 24, 104, 101, 97,
  100, 101, 114, 45, 107, 101, 121, 45, 48, 28, 104, 101, 97, 100, 101, 114, 45, 118, 97, 108, 117, 101, 45, 48, 24,
  104, 101, 97, 100, 101, 114, 45, 107, 101, 121, 45, 49, 28, 104, 101, 97, 100, 101, 114, 45, 118, 97, 108, 117, 101,
  45, 49, 24, 104, 101, 97, 100, 101, 114, 45, 107, 101, 121, 45, 49, 28, 104, 101, 97, 100, 101, 114, 45, 118, 97, 108,
  117, 101, 45, 50,
]);

// `encodeRecord()` writes its own varint length prefix (mirroring how a RecordBatch stores each
// record); `decodeRecord()` expects to be handed just the body, the way `decodeOneRecord` in
// batch.ts unwraps it via `readVarIntBytes()` before delegating.
function unwrapRecordBody(encoded: Buffer): Buffer {
  const body = new Decoder(encoded).readVarIntBytes();
  if (body === null) throw new Error('expected a non-null record body');
  return body;
}

function baseBatchContext(overrides: Partial<RecordBatchContext> = {}): RecordBatchContext {
  return {
    firstOffset: 0n,
    firstTimestamp: 1509827900073n,
    partitionLeaderEpoch: 0,
    inTransaction: false,
    isControlBatch: false,
    lastOffsetDelta: 0,
    producerId: -1n,
    producerEpoch: 0,
    firstSequence: 0,
    maxTimestamp: 1509827900073n,
    timestampType: TIMESTAMP_TYPES.CREATE_TIME,
    magicByte: 2,
    ...overrides,
  };
}

describe('protocol/records/record', () => {
  it('decodes a known-good fixture, including multi-value headers', () => {
    const batchContext = baseBatchContext();
    const decoded = decodeRecord(new Decoder(V0_RECORD_FIXTURE), batchContext);

    expect(decoded).toEqual({
      offset: 0n,
      magicByte: 2,
      attributes: 0,
      batchContext,
      timestamp: 1509827900073n,
      headers: {
        'header-key-0': Buffer.from('header-value-0'),
        'header-key-1': [Buffer.from('header-value-1'), Buffer.from('header-value-2')],
      },
      key: Buffer.from('key-0'),
      value: Buffer.from('some-value-0'),
      isControlRecord: false,
      byteSize: V0_RECORD_FIXTURE.length,
    });
  });

  it('uses the batch maxTimestamp when the topic uses LOG_APPEND_TIME', () => {
    const batchContext = baseBatchContext({
      maxTimestamp: 1597425188809n,
      timestampType: TIMESTAMP_TYPES.LOG_APPEND_TIME,
    });

    const decoded = decodeRecord(new Decoder(V0_RECORD_FIXTURE), batchContext);

    expect(decoded.batchContext).toEqual(batchContext);
    expect(decoded.timestamp).toEqual(batchContext.maxTimestamp);
  });

  it('propagates isControlBatch as isControlRecord', () => {
    const batchContext = baseBatchContext({ isControlBatch: true });
    const decoded = decodeRecord(new Decoder(V0_RECORD_FIXTURE), batchContext);
    expect(decoded.isControlRecord).toBe(true);
  });

  it('round-trips offsetDelta/timestampDelta, key, value and headers', () => {
    const encoded = encodeRecord({
      offsetDelta: 3,
      timestampDelta: 500n,
      key: Buffer.from('my-key'),
      value: Buffer.from('my-value'),
      headers: {
        single: 'one-value',
        repeated: ['first', 'second'],
      },
    }).buffer;

    const batchContext = baseBatchContext();
    const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), batchContext);

    expect(decoded.offset).toBe(0n + 3n);
    expect(decoded.timestamp).toBe(1509827900073n + 500n);
    expect(decoded.key).toEqual(Buffer.from('my-key'));
    expect(decoded.value).toEqual(Buffer.from('my-value'));
    expect(decoded.headers).toEqual({
      single: Buffer.from('one-value'),
      repeated: [Buffer.from('first'), Buffer.from('second')],
    });
  });

  it('round-trips a record with null key/value and no headers', () => {
    const encoded = encodeRecord({}).buffer;
    const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext());

    expect(decoded.key).toBeNull();
    expect(decoded.value).toBeNull();
    expect(decoded.headers).toEqual({});
  });

  it('sizes the record body exactly (no leftover bytes after decoding)', () => {
    const encoder = encodeRecord({ key: 'k', value: 'v' });
    const decoder = new Decoder(encoder.buffer);
    const length = decoder.readVarInt();
    expect(decoder.buffer.length - decoder.offset).toBe(length);
  });

  it('appends into a provided encoder instead of allocating a new one', () => {
    const encoder = new Encoder().writeInt8(7);
    const returned = encodeRecord({ key: 'k', value: 'v' }, encoder);
    expect(returned).toBe(encoder);
    expect(encoder.buffer[0]).toBe(7);

    const body = unwrapRecordBody(encoder.buffer.subarray(1));
    const decoded = decodeRecord(new Decoder(body), baseBatchContext());
    expect(decoded.key).toEqual(Buffer.from('k'));
    expect(decoded.value).toEqual(Buffer.from('v'));
  });

  describe('timestamp and offset', () => {
    it('computes a timestamp whose delta needs more than 7 varlong bytes', () => {
      const timestampDelta = 2n ** 53n + 1n;
      const encoded = encodeRecord({ offsetDelta: 7, timestampDelta }).buffer;
      const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext({ firstOffset: 10n }));

      expect(decoded.timestamp).toBe(1509827900073n + timestampDelta);
      expect(decoded.offset).toBe(17n);
    });

    it('computes a negative timestamp delta exactly', () => {
      const encoded = encodeRecord({ timestampDelta: -1000n }).buffer;
      const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext());

      expect(decoded.timestamp).toBe(1509827900073n - 1000n);
    });
  });

  describe('object shape', () => {
    const ALL_FIELDS = [
      'magicByte',
      'attributes',
      'timestamp',
      'offset',
      'key',
      'value',
      'headers',
      'isControlRecord',
      'batchContext',
      'byteSize',
    ];

    function decodeSample() {
      const encoded = encodeRecord({ offsetDelta: 3, key: 'k', value: 'v', headers: { h: 'hv' } }).buffer;
      return decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext());
    }

    it('exposes every field as an own enumerable data property', () => {
      const decoded = decodeSample();

      expect(Object.keys(decoded)).toEqual(ALL_FIELDS);
      for (const field of ALL_FIELDS) {
        expect(Object.getOwnPropertyDescriptor(decoded, field)).toHaveProperty('writable', true);
      }
    });

    it('keeps every field in object spread and structuredClone', () => {
      const decoded = decodeSample();
      const spread = { ...decoded, value: decoded.value?.toString() };
      const cloned = structuredClone(decoded);

      expect(spread).toMatchObject({ offset: 3n, timestamp: 1509827900073n, value: 'v' });
      expect(spread.headers).toEqual({ h: Buffer.from('hv') });
      expect(Object.keys(cloned)).toEqual(ALL_FIELDS);
      expect(cloned.offset).toBe(3n);
      expect(Buffer.from(cloned.value ?? [])).toEqual(Buffer.from('v'));
    });

    it('gives every record its own headers object, even with no headers', () => {
      const body = unwrapRecordBody(encodeRecord({}).buffer);
      const first = decodeRecord(new Decoder(body), baseBatchContext());
      const second = decodeRecord(new Decoder(body), baseBatchContext());

      expect(first.headers).toEqual({});
      expect(first.headers).not.toBe(second.headers);
      first.headers.added = 'x';
      expect(second.headers).toEqual({});
    });
  });

  describe('in-place decoding', () => {
    it('decodes consecutive records from one shared decoder and stops at each record end', () => {
      const first = unwrapRecordBody(encodeRecord({ offsetDelta: 0, key: 'k0', value: 'v0' }).buffer);
      const second = unwrapRecordBody(encodeRecord({ offsetDelta: 1, key: 'k1', value: 'v1' }).buffer);
      const decoder = new Decoder(Buffer.concat([first, second]));

      const firstRecord = decodeRecord(decoder, baseBatchContext(), first.length);
      expect(decoder.offset).toBe(first.length);
      const secondRecord = decodeRecord(decoder, baseBatchContext(), first.length + second.length);

      expect(firstRecord.byteSize).toBe(first.length);
      expect(secondRecord.byteSize).toBe(second.length);
      expect(firstRecord.value).toEqual(Buffer.from('v0'));
      expect(secondRecord.key).toEqual(Buffer.from('k1'));
      expect(secondRecord.value).toEqual(Buffer.from('v1'));
    });

    it('throws instead of reading the next record when the key overruns the record length', () => {
      const body = unwrapRecordBody(encodeRecord({ key: 'key', value: 'value' }).buffer);
      const keyEnd = 1 + 1 + 1 + 1 + 'key'.length; // attributes, deltas, key length, key bytes

      expect(() => decodeRecord(new Decoder(body), baseBatchContext(), keyEnd - 1)).toThrow(RangeError);
    });

    it('throws when the value overruns the record length, without reading the next record', () => {
      const body = unwrapRecordBody(encodeRecord({ key: 'key', value: 'value' }).buffer);

      expect(() => decodeRecord(new Decoder(body), baseBatchContext(), body.length - 2)).toThrow(RangeError);
    });

    it('rejects a negative key length instead of re-reading bytes of the previous record', () => {
      const first = unwrapRecordBody(encodeRecord({ key: 'k0', value: 'v0' }).buffer);
      const second = Buffer.from(unwrapRecordBody(encodeRecord({ key: 'k1', value: 'v1' }).buffer));
      const keyLengthIndex = 1 + 1 + 1; // after attributes and both deltas
      second[keyLengthIndex] = 0x09; // zigzag -5
      const decoder = new Decoder(Buffer.concat([first, second]));

      decodeRecord(decoder, baseBatchContext(), first.length);

      expect(() => decodeRecord(decoder, baseBatchContext(), first.length + second.length)).toThrow(RangeError);
    });

    it('stops at the record end when a corrupt header count runs into the next record', () => {
      const first = unwrapRecordBody(encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer);
      const second = unwrapRecordBody(encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer);
      const corrupted = Buffer.concat([first, second]);
      const headerCountIndex = 1 + 1 + 1 + 2 + 2; // after attributes, both deltas, key and value
      corrupted[headerCountIndex] = 0x7e; // zigzag 63 headers

      expect(() => decodeRecord(new Decoder(corrupted), baseBatchContext(), first.length)).toThrow(RangeError);
    });
  });

  describe('truncated records', () => {
    it('throws when the body ends right after the key', () => {
      const encoded = encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer;
      const body = unwrapRecordBody(encoded);

      const probe = new Decoder(body);
      probe.readInt8(); // attributes
      probe.readVarLong(); // timestampDelta
      probe.readVarInt(); // offsetDelta
      probe.readVarIntBytes(); // key
      const truncatedBody = body.subarray(0, probe.offset);

      expect(() => decodeRecord(new Decoder(truncatedBody), baseBatchContext())).toThrow(RangeError);
    });
  });
});
