import { describe, expect, it } from 'vitest'
import { Metadata } from './index.js'

describe('protocol/requests/metadata', () => {
  it('implements versions 0 through 6', () => {
    expect(Metadata.versions).toEqual([0, 1, 2, 3, 4, 5, 6])
  })

  it('defaults topics to [] and allowAutoTopicCreation to true', async () => {
    const { request } = Metadata.protocol({ version: 4 })({})
    const encoder = await request.encode()
    // topics=[] on v4+ collapses to wire length -1, then allowAutoTopicCreation=true (byte 1)
    expect(encoder.buffer).toEqual(Buffer.from([0xff, 0xff, 0xff, 0xff, 1]))
  })
})
