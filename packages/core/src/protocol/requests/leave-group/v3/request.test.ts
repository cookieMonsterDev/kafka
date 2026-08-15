import { describe, expect, it } from 'vitest'
import v3RequestFixture from '../fixtures/v3-request.json' with { type: 'json' }
import { leaveGroupRequestV3 } from './request.js'

describe('protocol/requests/leave-group/v3/request', () => {
  it('encodes a batch of members, matching the real fixture', async () => {
    const definition = leaveGroupRequestV3({
      groupId: 'consumer-group-id-82d77df5d0974e21502d-30919-0ec5e55e-e3e1-433a-bbed-96fe228408b4',
      members: [
        {
          memberId:
            'test-c598169a5d8dbedcb806-30919-ff1f3c53-1855-4c04-aadf-12d298160f5c-b41b37f8-6482-47c5-811e-e658ab656a75',
          groupInstanceId: null,
        },
      ],
    })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(Buffer.from(v3RequestFixture.data))
  })
})
