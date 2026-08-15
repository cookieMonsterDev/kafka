import { describe, expect, it } from 'vitest'
import { Decoder } from './decoder.js'
import { Encoder } from './encoder.js'

/**
 * Property-based round-trip coverage for the codec primitives: generate many random values,
 * encode then decode, and assert equality. A seeded LCG keeps failures reproducible.
 */
function makeRandom(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0
    return state / 0x100000000
  }
}

const ITERATIONS = 500
const random = makeRandom(0xc0ffee)

function randomInt32(): number {
  return Math.floor(random() * 0x100000000) - 0x80000000
}

function randomInt64(): bigint {
  const high = BigInt(randomInt32())
  const low = BigInt(Math.floor(random() * 0x100000000))
  return (high << 32n) | low
}

function randomString(): string {
  const length = Math.floor(random() * 32)
  let s = ''
  for (let i = 0; i < length; i++) {
    s += String.fromCharCode(32 + Math.floor(random() * 95))
  }
  return s
}

function randomBuffer(): Buffer {
  const length = Math.floor(random() * 64)
  const bytes: number[] = []
  for (let i = 0; i < length; i++) bytes.push(Math.floor(random() * 256))
  return Buffer.from(bytes)
}

describe('protocol round-trip properties', () => {
  it('writeVarInt/readVarInt round-trips arbitrary int32 values', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = randomInt32()
      const buffer = new Encoder().writeVarInt(value).buffer
      expect(new Decoder(buffer).readVarInt()).toBe(value)
    }
  })

  it('writeUVarInt/readUVarInt round-trips arbitrary uint32 values', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = Math.floor(random() * 0x100000000)
      const buffer = new Encoder().writeUVarInt(value).buffer
      expect(new Decoder(buffer).readUVarInt()).toBe(value)
    }
  })

  it('writeVarLong/readVarLong round-trips arbitrary int64 values', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = randomInt64()
      const buffer = new Encoder().writeVarLong(value).buffer
      expect(new Decoder(buffer).readVarLong()).toBe(value)
    }
  })

  it('writeInt64/readInt64 round-trips arbitrary int64 values', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = randomInt64()
      const buffer = new Encoder().writeInt64(value).buffer
      expect(new Decoder(buffer).readInt64()).toBe(value)
    }
  })

  it('writeString/readString round-trips arbitrary strings, including null', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = random() < 0.1 ? null : randomString()
      const buffer = new Encoder().writeString(value).buffer
      expect(new Decoder(buffer).readString()).toBe(value)
    }
  })

  it('writeUVarIntString/readUVarIntString round-trips arbitrary strings, including null', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = random() < 0.1 ? null : randomString()
      const buffer = new Encoder().writeUVarIntString(value).buffer
      expect(new Decoder(buffer).readUVarIntString()).toBe(value)
    }
  })

  it('writeBytes/readBytes round-trips arbitrary buffers, including null', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = random() < 0.1 ? null : randomBuffer()
      const buffer = new Encoder().writeBytes(value).buffer
      expect(new Decoder(buffer).readBytes()).toEqual(value)
    }
  })

  it('writeVarIntBytes/readVarIntBytes round-trips arbitrary buffers, including null', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = random() < 0.1 ? null : randomBuffer()
      const buffer = new Encoder().writeVarIntBytes(value).buffer
      expect(new Decoder(buffer).readVarIntBytes()).toEqual(value)
    }
  })

  it('writeArray/readArray round-trips arbitrary int32 arrays', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const length = Math.floor(random() * 10)
      const values = Array.from({ length }, randomInt32)
      const buffer = new Encoder().writeArray(values, 'int32').buffer
      expect(new Decoder(buffer).readArray((d) => d.readInt32())).toEqual(values)
    }
  })

  it('sizeOfVarInt always matches the actual encoded length', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = randomInt32()
      expect(Encoder.sizeOfVarInt(value)).toBe(new Encoder().writeVarInt(value).buffer.length)
    }
  })

  it('sizeOfVarLong always matches the actual encoded length', () => {
    for (let i = 0; i < ITERATIONS; i++) {
      const value = randomInt64()
      expect(Encoder.sizeOfVarLong(value)).toBe(new Encoder().writeVarLong(value).buffer.length)
    }
  })
})
