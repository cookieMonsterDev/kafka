import type { Decoder } from '../decoder';
import { Encoder } from '../encoder';
import type { TimestampType } from '../enums/timestamp-types';
import { TIMESTAMP_TYPES } from '../enums/timestamp-types';
import { decodeHeader, type RecordHeaderInput } from './header';

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
}

export function decodeRecord(decoder: Decoder, batchContext: RecordBatchContext): DecodedRecord {
  const { firstOffset, firstTimestamp, magicByte, isControlBatch, timestampType, maxTimestamp } = batchContext;
  const attributes = decoder.readInt8();

  const timestampDelta = decoder.readVarLong();
  const timestamp = timestampType === TIMESTAMP_TYPES.LOG_APPEND_TIME ? maxTimestamp : firstTimestamp + timestampDelta;

  const offsetDelta = decoder.readVarInt();
  const offset = firstOffset + BigInt(offsetDelta);

  const key = decoder.readVarIntBytes();
  const value = decoder.readVarIntBytes();

  const headers: Record<string, HeaderValue | HeaderValue[]> = {};
  for (const { key: headerKey, value: headerValue } of decoder.readVarIntArray(decodeHeader)) {
    const existing = headers[String(headerKey)];
    if (existing === undefined) {
      headers[String(headerKey)] = headerValue;
    } else if (Array.isArray(existing)) {
      existing.push(headerValue);
    } else {
      headers[String(headerKey)] = [existing, headerValue];
    }
  }

  return {
    magicByte,
    attributes,
    timestamp,
    offset,
    key,
    value,
    headers,
    isControlRecord: isControlBatch,
    batchContext,
  };
}
