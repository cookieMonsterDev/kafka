import { describe, expect, it } from 'vitest'
import v0RequestFixture from '../fixtures/v0-request.json' with { type: 'json' }
import { findCoordinatorRequestV0 } from './request.js'

describe('protocol/requests/find-coordinator/v0/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = findCoordinatorRequestV0({ groupId: 'test-topic' })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(Buffer.from(v0RequestFixture.data))
  })
})
