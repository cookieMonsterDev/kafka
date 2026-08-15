import { describe, expect, it } from 'vitest'
import { metadataRequestV3 } from './request.js'

describe('protocol/requests/metadata/v3/request', () => {
  it('carries apiVersion 3', () => {
    expect(metadataRequestV3({ topics: [] }).apiVersion).toBe(3)
  })
})
