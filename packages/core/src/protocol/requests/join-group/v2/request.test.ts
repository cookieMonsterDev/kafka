import { describe, expect, it } from 'vitest'
import v2AssignerMetadataFixture from '../fixtures/v2-assigner-metadata.json' with { type: 'json' }
import v2RequestFixture from '../fixtures/v2-request.json' with { type: 'json' }
import { joinGroupRequestV2 } from './request.js'

describe('protocol/requests/join-group/v2/request', () => {
  it('encodes to match the real fixture', async () => {
    const definition = joinGroupRequestV2({
      groupId: 'consumer-group-id-b522188a3a12a1f04cfb-23702-e1ff35c7-fde9-4d58-960a-2cef8af77eef',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [{ name: 'AssignerName', metadata: Buffer.from(v2AssignerMetadataFixture.data) }],
    })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(Buffer.from(v2RequestFixture.data))
  })
})
