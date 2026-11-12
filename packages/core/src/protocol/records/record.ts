import { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import type { TimestampType } from '../enums/timestamp-types';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import type { RecordHeaderInput } from './header';

/**
 * v2
 * Record =>
 *   Length => Varint
 *   Attributes => Int8
 *   TimestampDelta => Varlong
 *   OffsetDelta => Varint
 *   Key => varInt|Bytes
 *   Value => varInt|Bytes
 *   Headers => [HeaderKey HeaderValue]
 *     HeaderKey => VarInt|String
 *     HeaderValue => VarInt|Bytes
 */

export type HeaderValue = Buffer | string | null;

export interface RecordHeaders {
  [key: string]: HeaderValue | HeaderValue[];
}

export interface EncodeRecordOptions {
  offsetDelta?: number;
  timestampDelta?: bigint;
  key?: Buffer | string | null;
  value?: Buffer | string | null;
  headers?: RecordHeaders;
}

function flattenHeaders(headers: RecordHeaders): RecordHeaderInput[] {
  return Object.entries(headers).flatMap(([key, value]) =>
    Array.isArray(value) ? value.map((v) => ({ key, value: v })) : [{ key, value }],
  );
}

function sizeOfHeaders(headersArray: readonly RecordHeaderInput[]): number {
  let size = Encoder.sizeOfVarInt(headersArray.length);

  for (const header of headersArray) {
    const keySize = Buffer.byteLength(header.key);
    size += Encoder.sizeOfVarInt(keySize) + keySize;

    if (header.value === null) {
      size += Encoder.sizeOfVarInt(-1);
    } else {
      const valueSize = Buffer.byteLength(header.value);
      size += Encoder.sizeOfVarInt(valueSize) + valueSize;
    }
  }

  return size;
}

function writeHeaders(encoder: Encoder, headersArray: readonly RecordHeaderInput[]): void {
  encoder.writeVarInt(headersArray.length);
  for (const header of headersArray) {
    encoder.writeVarIntString(header.key);
    encoder.writeVarIntBytes(header.value);
  }
}

/**
 * Encode one v2 Record. When `encoder` is provided, bytes are appended in place (RecordBatch
 * hot path). Otherwise a new Encoder is allocated, sized to the record.
 */
export function encodeRecord(
  { offsetDelta = 0, timestampDelta = 0n, key = null, value = null, headers = {} }: EncodeRecordOptions = {},
  encoder?: Encoder,
): Encoder {
  const headersArray = flattenHeaders(headers);

  const sizeOfBody =
    1 + // always one byte for attributes
    Encoder.sizeOfVarLong(timestampDelta) +
    Encoder.sizeOfVarInt(offsetDelta) +
    Encoder.sizeOfVarIntBytes(key) +
    Encoder.sizeOfVarIntBytes(value) +
    sizeOfHeaders(headersArray);

  const target = encoder ?? new Encoder(Encoder.sizeOfVarInt(sizeOfBody) + sizeOfBody);
  target
    .writeVarInt(sizeOfBody)
    .writeInt8(0) // no used record attributes at the moment
    .writeVarLong(timestampDelta)
    .writeVarInt(offsetDelta)
    .writeVarIntBytes(key)
    .writeVarIntBytes(value);
  writeHeaders(target, headersArray);
  return target;
}

export interface RecordBatchContext {
  firstOffset: bigint;
  firstTimestamp: bigint;
  partitionLeaderEpoch: number;
  inTransaction: boolean;
  isControlBatch: boolean;
  lastOffsetDelta: number;
  producerId: bigint;
  producerEpoch: number;
  firstSequence: number;
  maxTimestamp: bigint;
  timestampType: TimestampType;
  /**
   * The magic byte is read by the Fetch protocol to distinguish the record batch from the
   * legacy message set; it's carried through the batch context rather than used directly.
   */
  magicByte: number;
}

/**
 * A decoded record: a plain object whose ten fields are all own, enumerable data properties, so
 * object spread, `Object.keys()`, `structuredClone()`, and `JSON.stringify()` see every field.
 */
export interface DecodedRecord {
  magicByte: number;
  attributes: number;
  timestamp: bigint;
  offset: bigint;
  key: Buffer | null;
  value: Buffer | null;
  headers: Record<string, HeaderValue | HeaderValue[]>;
  isControlRecord: boolean;
  batchContext: RecordBatchContext;
  /**
   * This record's on-wire size (post-decompression): attributes, timestamp/offset deltas, key,
   * value, and headers, all together - excluding the record's own length-prefix varint. It is the
   * length already read off the wire to isolate this record's bytes, so byte-usage accounting (e.g.
   * the consumer's adaptive fetch-size controller) can use it without measuring the fields.
   */
  byteSize: number;
}

function throwRecordOverrun(recordStart: number, recordEnd: number): never {
  throw new RangeError(`Record fields overran the record length (${recordEnd - recordStart} bytes)`);
}

/** Always returns a fresh object, even with no headers, because callers may mutate it. */
function decodeHeaders(
  decoder: Decoder,
  recordStart: number,
  recordEnd: number,
): Record<string, HeaderValue | HeaderValue[]> {
  const headers: Record<string, HeaderValue | HeaderValue[]> = {};
  const headerCount = decoder.readVarInt();
  for (let i = 0; i < headerCount; i++) {
    const headerKey = String(decoder.readVarIntString());
    const headerValue = decoder.readVarIntBytes();
    // Checked per header so a corrupt header count stops at the record end instead of parsing
    // the following records as headers.
    if (decoder.offset > recordEnd) throwRecordOverrun(recordStart, recordEnd);
    const existing = headers[headerKey];
    if (existing === undefined) {
      headers[headerKey] = headerValue;
    } else if (Array.isArray(existing)) {
      existing.push(headerValue);
    } else {
      headers[headerKey] = [existing, headerValue];
    }
  }
  return headers;
}

/**
 * Decodes one record body that runs from `decoder.offset` to `recordEnd` (the end of the buffer by
 * default) and leaves `decoder` at `recordEnd`. Decoding reads the shared batch decoder in place,
 * with no per-record decoder, and throws a `RangeError` when the fields overrun `recordEnd`.
 *
 * The result is an object literal with no accessors, built with its fields in one fixed order, so
 * every record shares one V8 hidden class. Getters on a literal are defined through a runtime call
 * with fresh closures per record, which moved each record into slow dictionary mode.
 */
export function decodeRecord(
  decoder: Decoder,
  batchContext: RecordBatchContext,
  recordEnd: number = decoder.buffer.length,
): DecodedRecord {
  const recordStart = decoder.offset;
  const attributes = decoder.readInt8();
  const timestampDelta = decoder.readVarLongNumberOrBigInt();
  const offsetDelta = decoder.readVarInt();
  const key = decoder.readVarIntBytes();
  const value = decoder.readVarIntBytes();
  if (decoder.offset > recordEnd) throwRecordOverrun(recordStart, recordEnd);
  const headers = decodeHeaders(decoder, recordStart, recordEnd);
  if (decoder.offset > recordEnd) throwRecordOverrun(recordStart, recordEnd);
  decoder.offset = recordEnd;

  const { firstOffset, firstTimestamp, maxTimestamp, timestampType } = batchContext;
  const timestamp =
    timestampType === TIMESTAMP_TYPES.LOG_APPEND_TIME ? maxTimestamp : firstTimestamp + BigInt(timestampDelta);

  return {
    magicByte: batchContext.magicByte,
    attributes,
    timestamp,
    offset: firstOffset + BigInt(offsetDelta),
    key,
    value,
    headers,
    isControlRecord: batchContext.isControlBatch,
    batchContext,
    byteSize: recordEnd - recordStart,
  };
}
