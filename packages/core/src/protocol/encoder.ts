const INT8_SIZE = 1
const INT16_SIZE = 2
const INT32_SIZE = 4
const INT64_SIZE = 8
const DOUBLE_SIZE = 8

const MOST_SIGNIFICANT_BIT = 0x80 // 128
const OTHER_BITS = 0x7f // 127
const UNSIGNED_INT32_MAX_NUMBER = 0xffffff80
const UNSIGNED_INT64_MAX_NUMBER = 0xffffffffffffff80n

export type ArrayElementType = 'int32' | 'number' | 'string' | 'object'
export type EncodableArrayValue = number | string | Encoder

function encodeZigZag(value: number): number {
  return (value << 1) ^ (value >> 31)
}

/** Zigzag-encodes a 64-bit value. Always non-negative, so later 7-bit shifts need no sign handling. */
function encodeZigZag64(value: bigint): bigint {
  return (value << 1n) ^ (value >> 63n)
}

/**
 * Wire-format writer for the Kafka protocol: fixed-width ints, strings/bytes (both length-prefixed
 * and varint-prefixed "compact" forms), varint/varlong, and arrays. Grows its backing buffer by
 * doubling, matching kafkajs's allocation strategy so encoded output stays byte-for-byte identical.
 */
export class Encoder {
  static encodeZigZag = encodeZigZag
  static encodeZigZag64 = encodeZigZag64

  static sizeOfVarInt(value: number): number {
    let encodedValue = encodeZigZag(value)
    let bytes = 1

    while ((encodedValue & UNSIGNED_INT32_MAX_NUMBER) !== 0) {
      bytes += 1
      encodedValue >>>= 7
    }

    return bytes
  }

  static sizeOfVarLong(value: bigint): number {
    let longValue = encodeZigZag64(value)
    let bytes = 1

    while ((longValue & UNSIGNED_INT64_MAX_NUMBER) !== 0n) {
      bytes += 1
      longValue >>= 7n
    }

    return bytes
  }

  static sizeOfVarIntBytes(value: Buffer | string | null | undefined): number {
    const size = value == null ? -1 : Buffer.byteLength(value)

    if (size < 0) {
      return Encoder.sizeOfVarInt(-1)
    }

    return Encoder.sizeOfVarInt(size) + size
  }

  static nextPowerOfTwo(value: number): number {
    return 1 << (31 - Math.clz32(value) + 1)
  }

  #buf: Buffer
  #offset: number

  constructor(initialSize = 511) {
    this.#buf = Buffer.alloc(Encoder.nextPowerOfTwo(initialSize))
    this.#offset = 0
  }

  #writeBufferInternal(buffer: Buffer): void {
    const bufferLength = buffer.length
    this.#ensureAvailable(bufferLength)
    buffer.copy(this.#buf, this.#offset, 0)
    this.#offset += bufferLength
  }

  #ensureAvailable(length: number): void {
    if (this.#offset + length > this.#buf.length) {
      const newLength = Encoder.nextPowerOfTwo(this.#offset + length)
      const newBuffer = Buffer.alloc(newLength)
      this.#buf.copy(newBuffer, 0, 0, this.#offset)
      this.#buf = newBuffer
    }
  }

  get buffer(): Buffer {
    return this.#buf.subarray(0, this.#offset)
  }

  writeInt8(value: number): this {
    this.#ensureAvailable(INT8_SIZE)
    this.#buf.writeInt8(value, this.#offset)
    this.#offset += INT8_SIZE
    return this
  }

  writeInt16(value: number): this {
    this.#ensureAvailable(INT16_SIZE)
    this.#buf.writeInt16BE(value, this.#offset)
    this.#offset += INT16_SIZE
    return this
  }

  writeInt32(value: number): this {
    this.#ensureAvailable(INT32_SIZE)
    this.#buf.writeInt32BE(value, this.#offset)
    this.#offset += INT32_SIZE
    return this
  }

  writeUInt32(value: number): this {
    this.#ensureAvailable(INT32_SIZE)
    this.#buf.writeUInt32BE(value, this.#offset)
    this.#offset += INT32_SIZE
    return this
  }

  /** Accepts a `number` for convenience (e.g. `Date.now()` timestamps); offsets/ids pass `bigint`. */
  writeInt64(value: bigint | number): this {
    this.#ensureAvailable(INT64_SIZE)
    this.#buf.writeBigInt64BE(BigInt(value), this.#offset)
    this.#offset += INT64_SIZE
    return this
  }

  writeDouble(value: number): this {
    this.#ensureAvailable(DOUBLE_SIZE)
    this.#buf.writeDoubleBE(value, this.#offset)
    this.#offset += DOUBLE_SIZE
    return this
  }

  writeBoolean(value: boolean): this {
    return this.writeInt8(value ? 1 : 0)
  }

  writeString(value: string | null | undefined): this {
    if (value == null) {
      return this.writeInt16(-1)
    }

    const byteLength = Buffer.byteLength(value, 'utf8')
    this.#ensureAvailable(INT16_SIZE + byteLength)
    this.writeInt16(byteLength)
    this.#buf.write(value, this.#offset, byteLength, 'utf8')
    this.#offset += byteLength
    return this
  }

  writeVarIntString(value: string | null | undefined): this {
    if (value == null) {
      return this.writeVarInt(-1)
    }

    const byteLength = Buffer.byteLength(value, 'utf8')
    this.writeVarInt(byteLength)
    this.#ensureAvailable(byteLength)
    this.#buf.write(value, this.#offset, byteLength, 'utf8')
    this.#offset += byteLength
    return this
  }

  writeUVarIntString(value: string | null | undefined): this {
    if (value == null) {
      return this.writeUVarInt(0)
    }

    const byteLength = Buffer.byteLength(value, 'utf8')
    this.writeUVarInt(byteLength + 1)
    this.#ensureAvailable(byteLength)
    this.#buf.write(value, this.#offset, byteLength, 'utf8')
    this.#offset += byteLength
    return this
  }

  writeBytes(value: Buffer | string | null | undefined): this {
    if (value == null) {
      return this.writeInt32(-1)
    }

    if (Buffer.isBuffer(value)) {
      this.#ensureAvailable(INT32_SIZE + value.length)
      this.writeInt32(value.length)
      this.#writeBufferInternal(value)
    } else {
      const byteLength = Buffer.byteLength(value, 'utf8')
      this.#ensureAvailable(INT32_SIZE + byteLength)
      this.writeInt32(byteLength)
      this.#buf.write(value, this.#offset, byteLength, 'utf8')
      this.#offset += byteLength
    }

    return this
  }

  writeVarIntBytes(value: Buffer | string | null | undefined): this {
    if (value == null) {
      return this.writeVarInt(-1)
    }

    if (Buffer.isBuffer(value)) {
      this.writeVarInt(value.length)
      this.#writeBufferInternal(value)
    } else {
      const byteLength = Buffer.byteLength(value, 'utf8')
      this.writeVarInt(byteLength)
      this.#ensureAvailable(byteLength)
      this.#buf.write(value, this.#offset, byteLength, 'utf8')
      this.#offset += byteLength
    }

    return this
  }

  writeUVarIntBytes(value: Buffer | string | null | undefined): this {
    if (value == null) {
      return this.writeVarInt(0)
    }

    if (Buffer.isBuffer(value)) {
      this.writeUVarInt(value.length + 1)
      this.#writeBufferInternal(value)
    } else {
      const byteLength = Buffer.byteLength(value, 'utf8')
      this.writeUVarInt(byteLength + 1)
      this.#ensureAvailable(byteLength)
      this.#buf.write(value, this.#offset, byteLength, 'utf8')
      this.#offset += byteLength
    }

    return this
  }

  writeEncoder(value: Encoder): this {
    this.#writeBufferInternal(value.buffer)
    return this
  }

  writeEncoderArray(value: readonly Encoder[]): this {
    for (const v of value) this.#writeBufferInternal(v.buffer)
    return this
  }

  writeBuffer(value: Buffer): this {
    this.#writeBufferInternal(value)
    return this
  }

  /** A null value is encoded as length -1 with no following bytes; empty array and null are equivalent here. */
  writeNullableArray(array: readonly EncodableArrayValue[], type?: ArrayElementType): this {
    const length = array.length !== 0 ? array.length : -1
    return this.writeArray(array, type, length)
  }

  writeArray(array: readonly EncodableArrayValue[], type?: ArrayElementType, length?: number): this {
    const arrayLength = length == null ? array.length : length
    this.writeInt32(arrayLength)

    if (type !== undefined) {
      switch (type) {
        case 'int32':
        case 'number':
          for (const value of array) this.writeInt32(value as number)
          break
        case 'string':
          for (const value of array) this.writeString(value as string)
          break
        case 'object':
          this.writeEncoderArray(array as readonly Encoder[])
          break
      }
    } else {
      for (const value of array) {
        switch (typeof value) {
          case 'number':
            this.writeInt32(value)
            break
          case 'string':
            this.writeString(value)
            break
          case 'object':
            this.writeEncoder(value)
            break
        }
      }
    }

    return this
  }

  writeVarIntArray(array: readonly EncodableArrayValue[], type?: ArrayElementType): this {
    if (type === 'object') {
      this.writeVarInt(array.length)
      this.writeEncoderArray(array as readonly Encoder[])
    } else {
      const objectArray = array.filter((v): v is Encoder => typeof v === 'object')
      this.writeVarInt(objectArray.length)
      this.writeEncoderArray(objectArray)
    }
    return this
  }

  writeUVarIntArray(array: readonly EncodableArrayValue[] | null, type?: ArrayElementType): this {
    if (type === 'object' && array !== null) {
      this.writeUVarInt(array.length + 1)
      this.writeEncoderArray(array as readonly Encoder[])
    } else if (array === null) {
      this.writeUVarInt(0)
    } else {
      const objectArray = array.filter((v): v is Encoder => typeof v === 'object')
      this.writeUVarInt(objectArray.length + 1)
      this.writeEncoderArray(objectArray)
    }
    return this
  }

  // Based on: https://en.wikipedia.org/wiki/LEB128 (using LEB128 format similar to VLQ), and
  // https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/util/Varint.java#L106
  writeVarInt(value: number): this {
    return this.writeUVarInt(encodeZigZag(value))
  }

  writeUVarInt(value: number): this {
    const byteArray: number[] = []
    while ((value & UNSIGNED_INT32_MAX_NUMBER) !== 0) {
      byteArray.push((value & OTHER_BITS) | MOST_SIGNIFICANT_BIT)
      value >>>= 7
    }
    byteArray.push(value & OTHER_BITS)
    this.#writeBufferInternal(Buffer.from(byteArray))
    return this
  }

  writeVarLong(value: bigint): this {
    const byteArray: number[] = []
    let longValue = encodeZigZag64(value)

    while ((longValue & UNSIGNED_INT64_MAX_NUMBER) !== 0n) {
      byteArray.push(Number(BigInt.asIntN(32, (longValue & BigInt(OTHER_BITS)) | BigInt(MOST_SIGNIFICANT_BIT))))
      longValue >>= 7n
    }

    byteArray.push(Number(BigInt.asIntN(32, longValue)))
    this.#writeBufferInternal(Buffer.from(byteArray))
    return this
  }

  // We can use the offset directly: the buffer is never re-encoded once written.
  size(): number {
    return this.#offset
  }

  toJSON(): ReturnType<Buffer['toJSON']> {
    return this.buffer.toJSON()
  }
}
