import { describe, expect, it } from 'vitest'
import v1AssignerMetadataFixture from '../fixtures/v1-assigner-metadata.json' with { type: 'json' }
import v1RequestFixture from '../fixtures/v1-request.json' with { type: 'json' }
import { joinGroupRequestV1 } from './request.js'

describe('protocol/requests/join-group/v1/request', () => {
  it('encodes to match the real fixture, including rebalanceTimeout', async () => {
    const definition = joinGroupRequestV1({
      groupId: 'consumer-group-id-5d520373e1cf4d03ca77-21486-90948f57-528c-4c3b-ba72-bf1e0d9bbc56',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [{ name: 'AssignerName', metadata: Buffer.from(v1AssignerMetadataFixture.data) }],
    })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(Buffer.from(v1RequestFixture.data))
  })
})
