import { describe, expect, it } from 'vitest'
import { Encoder } from '../../../encoder.js'
import { metadataRequestV0 } from './request.js'

describe('protocol/requests/metadata/v0/request', () => {
  it('encodes topics as a plain (non-nullable) array', async () => {
    const topics = ['test-topic-1', 'test-topic-2']
    const definition = metadataRequestV0({ topics })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(new Encoder().writeArray(topics).buffer)
  })

  it('encodes an empty topics array as wire length 0, not -1', async () => {
    const definition = metadataRequestV0({ topics: [] })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(new Encoder().writeInt32(0).buffer)
  })
})
