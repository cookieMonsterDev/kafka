import { describe, expect, expectTypeOf, it } from 'vitest'
import { Decoder } from './decoder.js'
import { Encoder } from './encoder.js'
import {
  array,
  boolean,
  field,
  int16,
  int32,
  int64,
  nullableString,
  object,
  string,
} from './schema.js'

describe('protocol/schema', () => {
  it('round-trips a flat object', () => {
    const shape = object([field('a', int16), field('b', boolean), field('c', string)])
    const encoder = new Encoder()
    shape.write(encoder, { a: 7, b: true, c: 'hello' })

    expectTypeOf<ReturnType<typeof shape.read>>().toEqualTypeOf<{ a: number; b: boolean; c: string }>()

    const decoded = shape.read(new Decoder(encoder.buffer))
    expect(decoded).toEqual({ a: 7, b: true, c: 'hello' })
  })

  it('round-trips nested objects and arrays of objects, in field order', () => {
    const partition = object([field('partitionId', int32), field('leader', int32)])
    const topic = object([
      field('topic', string),
      field('rack', nullableString),
      field('partitions', array(partition)),
    ])

    const value = {
      topic: 'my-topic',
      rack: null,
      partitions: [
        { partitionId: 0, leader: 1 },
        { partitionId: 1, leader: 2 },
      ],
    }

    const encoder = new Encoder()
    topic.write(encoder, value)
    const decoded = topic.read(new Decoder(encoder.buffer))
    expect(decoded).toEqual(value)
  })

  it('round-trips an empty array and a bigint field', () => {
    const shape = object([field('offsets', array(int64)), field('count', int32)])
    const encoder = new Encoder()
    shape.write(encoder, { offsets: [], count: 0 })
    expect(shape.read(new Decoder(encoder.buffer))).toEqual({ offsets: [], count: 0 })

    const withValues = { offsets: [1n, -2n, 9007199254740993n], count: 3 }
    const encoder2 = new Encoder()
    shape.write(encoder2, withValues)
    expect(shape.read(new Decoder(encoder2.buffer))).toEqual(withValues)
  })

  it('throws when a non-nullable string field is null on the wire', () => {
    const encoder = new Encoder().writeString(null)
    expect(() => string.read(new Decoder(encoder.buffer))).toThrow(RangeError)
  })
})
