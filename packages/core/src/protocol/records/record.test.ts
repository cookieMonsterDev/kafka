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

  describe('lazy value/headers', () => {
    it('accessing headers first decodes it correctly, without reading value first', () => {
      const encoded = encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer;
      const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext());

      expect(decoded.headers).toEqual({ h: Buffer.from('hv') });
      expect(decoded.value).toEqual(Buffer.from('v'));
    });

    it('memoizes value/headers instead of re-parsing on every access', () => {
      const encoded = encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer;
      const decoded = decodeRecord(new Decoder(unwrapRecordBody(encoded)), baseBatchContext());

      const value1 = decoded.value;
      const value2 = decoded.value;
      expect(value1).toBe(value2);

      const headers1 = decoded.headers;
      const headers2 = decoded.headers;
      expect(headers1).toBe(headers2);
    });

    it('does not decode value/headers until accessed', () => {
      // Truncate the encoded body right after `key` — offset/attributes/key survive `decodeRecord`
      // untouched, but nothing is left for value/headers to parse from.
      const encoded = encodeRecord({ key: 'k', value: 'v', headers: { h: 'hv' } }).buffer;
      const body = unwrapRecordBody(encoded);

      const probe = new Decoder(body);
      probe.readInt8(); // attributes
      probe.readVarLong(); // timestampDelta
      probe.readVarInt(); // offsetDelta
      probe.readVarIntBytes(); // key
      const truncatedBody = body.subarray(0, probe.offset);

      const decoded = decodeRecord(new Decoder(truncatedBody), baseBatchContext());

      // Decoding the record itself, and reading its cheap fields, must not throw.
      expect(decoded.key).toEqual(Buffer.from('k'));
      expect(decoded.offset).toBe(0n);

      // Only once `value` (or `headers`) is actually read does the missing data surface.
      expect(() => decoded.value).toThrow();
      expect(() => decoded.headers).toThrow();
    });
  });
});
