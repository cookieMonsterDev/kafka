import { describe, expect, it } from 'vitest'
import { Encoder } from '../encoder.js'
import { gzipCodec } from './gzip.js'

describe('protocol/compression/gzip', () => {
  it('round-trips arbitrary bytes', async () => {
    const encoder = new Encoder().writeString('hello kafka').writeInt32(42)
    const compressed = await gzipCodec.compress(encoder)
    const decompressed = await gzipCodec.decompress(compressed)
    expect(decompressed).toEqual(encoder.buffer)
  })

  it('actually compresses (output looks like gzip, not raw passthrough)', async () => {
    const encoder = new Encoder().writeBuffer(Buffer.alloc(1024, 'a'))
    const compressed = await gzipCodec.compress(encoder)
    expect(compressed[0]).toBe(0x1f)
    expect(compressed[1]).toBe(0x8b)
  })
})
