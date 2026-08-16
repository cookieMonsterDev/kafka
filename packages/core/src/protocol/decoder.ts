import { KafkaInvalidVarIntError, KafkaInvalidLongError } from '../errors';

const INT8_SIZE = 1;
const INT16_SIZE = 2;
const INT32_SIZE = 4;
const INT64_SIZE = 8;
const DOUBLE_SIZE = 8;

const MOST_SIGNIFICANT_BIT = 0x80; // 128
const OTHER_BITS = 0x7f; // 127

function decodeZigZag(value: number): number {
  return (value >>> 1) ^ -(value & 1);
}

/** `longValue` is always non-negative here (it only ever holds a zigzag-encoded varlong in progress). */
function decodeZigZag64(longValue: bigint): bigint {
  const negatedLowBit = (longValue & 1n) === 0n ? 0n : -1n;
  return (longValue >> 1n) ^ negatedLowBit;
}

export interface ArrayReader<T> {
  (decoder: Decoder): T;
}

/**
 * Wire-format reader for the Kafka protocol. Every read advances an internal offset;
 * out-of-range reads throw rather than returning `undefined`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export class Decoder {
  static int32Size(): number {
    return INT32_SIZE;
  }

  readonly buffer: Buffer;
  offset: number;

  constructor(buffer: Buffer) {
    this.buffer = buffer;
    this.offset = 0;
  }

  #readByte(offset: number): number {
    const byte = this.buffer[offset];
    if (byte === undefined) {
      throw new RangeError(`Tried to read past the end of the buffer (offset ${offset})`);
    }
    return byte;
  }

  readInt8(): number {
    const value = this.buffer.readInt8(this.offset);
    this.offset += INT8_SIZE;
    return value;
  }

  canReadInt16(): boolean {
    return this.canReadBytes(INT16_SIZE);
  }

  readInt16(): number {
    const value = this.buffer.readInt16BE(this.offset);
    this.offset += INT16_SIZE;
    return value;
  }

  canReadInt32(): boolean {
    return this.canReadBytes(INT32_SIZE);
  }

  readInt32(): number {
    const value = this.buffer.readInt32BE(this.offset);
    this.offset += INT32_SIZE;
    return value;
  }

  canReadInt64(): boolean {
    return this.canReadBytes(INT64_SIZE);
  }

  readInt64(): bigint {
    const value = this.buffer.readBigInt64BE(this.offset);
    this.offset += INT64_SIZE;
    return value;
  }

  readDouble(): number {
    const value = this.buffer.readDoubleBE(this.offset);
    this.offset += DOUBLE_SIZE;
    return value;
  }

  readString(): string | null {
    const byteLength = this.readInt16();

    if (byteLength === -1) {
      return null;
    }

    const value = this.buffer.toString('utf8', this.offset, this.offset + byteLength);
    this.offset += byteLength;
    return value;
  }

  readVarIntString(): string | null {
    const byteLength = this.readVarInt();

    if (byteLength === -1) {
      return null;
    }

    const value = this.buffer.toString('utf8', this.offset, this.offset + byteLength);
    this.offset += byteLength;
    return value;
  }

  readUVarIntString(): string | null {
    const byteLength = this.readUVarInt();

    if (byteLength === 0) {
      return null;
    }

    const value = this.buffer.toString('utf8', this.offset, this.offset + byteLength - 1);
    this.offset += byteLength - 1;
    return value;
  }

  canReadBytes(length: number): boolean {
    return Buffer.byteLength(this.buffer) - this.offset >= length;
  }

  readBytes(byteLength: number = this.readInt32()): Buffer | null {
    if (byteLength === -1) {
      return null;
    }

    const value = this.buffer.subarray(this.offset, this.offset + byteLength);
    this.offset += byteLength;
    return value;
  }

  readVarIntBytes(): Buffer | null {
    const byteLength = this.readVarInt();

    if (byteLength === -1) {
      return null;
    }

    const value = this.buffer.subarray(this.offset, this.offset + byteLength);
    this.offset += byteLength;
    return value;
  }

  readUVarIntBytes(): Buffer | null {
    const byteLength = this.readUVarInt();

    if (byteLength === 0) {
      return null;
    }

    const value = this.buffer.subarray(this.offset, this.offset + byteLength);
    this.offset += byteLength - 1;
    return value;
  }

  readBoolean(): boolean {
    return this.readInt8() === 1;
  }

  readAll(): Buffer {
    const result = this.buffer.subarray(this.offset);
    this.offset += Buffer.byteLength(this.buffer);
    return result;
  }

  readArray<T>(reader: ArrayReader<T>): T[] {
    const length = this.readInt32();

    if (length === -1) {
      return [];
    }

    const array: T[] = new Array<T>(length);
    for (let i = 0; i < length; i++) {
      array[i] = reader(this);
    }

    return array;
  }

  readVarIntArray<T>(reader: ArrayReader<T>): T[] {
    const length = this.readVarInt();

    if (length === -1) {
      return [];
    }

    const array: T[] = new Array<T>(length);
    for (let i = 0; i < length; i++) {
      array[i] = reader(this);
    }

    return array;
  }

  /**
   * Per the protocol type documentation (https://kafka.apache.org/protocol#protocol_types), a
   * compact array with length zero is a null array; a length of 1 is an empty array.
   */
  readUVarIntArray<T>(reader: ArrayReader<T>): T[] | null {
    const length = this.readUVarInt();

    if (length === 0) {
      return null;
    }

    const array: T[] = new Array<T>(length - 1);
    for (let i = 0; i < length - 1; i++) {
      array[i] = reader(this);
    }

    return array;
  }

  async readArrayAsync<T>(reader: (decoder: Decoder) => Promise<T>): Promise<T[]> {
    const length = this.readInt32();

    if (length === -1) {
      return [];
    }

    const array: T[] = new Array<T>(length);
    for (let i = 0; i < length; i++) {
      array[i] = await reader(this);
    }

    return array;
  }

  readVarInt(): number {
    let currentByte: number;
    let result = 0;
    let i = 0;

    do {
      currentByte = this.#readByte(this.offset++);
      result += (currentByte & OTHER_BITS) << i;
      i += 7;
    } while (currentByte >= MOST_SIGNIFICANT_BIT);

    return decodeZigZag(result);
  }

  // JavaScript numbers are float64; bitwise ops coerce to a signed 32-bit int, so `>>>` is used
  // to bring the accumulated result back to an unsigned 32-bit value.
  readUVarInt(): number {
    let currentByte = this.#readByte(this.offset++);
    let result = 0;
    let i = 0;

    while ((currentByte & MOST_SIGNIFICANT_BIT) !== 0) {
      result |= (currentByte & OTHER_BITS) << i;
      i += 7;
      if (i > 28) {
        throw new KafkaInvalidVarIntError('Invalid VarInt, must contain 5 bytes or less');
      }
      currentByte = this.#readByte(this.offset++);
    }

    result |= currentByte << i;
    return result >>> 0;
  }

  readTaggedFields(): Record<string, unknown> | null {
    const numberOfTaggedFields = this.readUVarInt();

    if (numberOfTaggedFields === 0) {
      return null;
    }

    // Reads tag, field length, and then that many bytes for the field value, skipping over the tag.
    for (let i = 0; i < numberOfTaggedFields; i++) {
      this.readUVarInt();
      this.readUVarIntBytes();
    }

    return {};
  }

  readVarLong(): bigint {
    let currentByte: number;
    let result = 0n;
    let i = 0n;

    do {
      if (i > 63n) {
        throw new KafkaInvalidLongError('Invalid Long, must contain 9 bytes or less');
      }
      currentByte = this.#readByte(this.offset++);
      result += BigInt(currentByte & OTHER_BITS) << i;
      i += 7n;
    } while (currentByte >= MOST_SIGNIFICANT_BIT);

    return decodeZigZag64(result);
  }

  slice(size: number): Decoder {
    return new Decoder(this.buffer.subarray(this.offset, this.offset + size));
  }

  forward(size: number): void {
    this.offset += size;
  }
}
