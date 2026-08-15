import { describe, expect, it } from 'vitest'
import v2AssignerMetadataFixture from '../fixtures/v2-assigner-metadata.json' with { type: 'json' }
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' }
import { joinGroupResponseV5 } from './response.js'

describe('protocol/requests/join-group/v5/response', () => {
  it('decodes a real fixture, including group_instance_id per member', async () => {
    const data = await joinGroupResponseV5.decode(Buffer.from(v5ResponseFixture.data))

    const memberId =
      'test-b773bdb220aa2b862440-23702-2b1581f6-55ea-4af0-97f0-931d4f071111-68a2051d-7b30-4161-b920-89346d7b672b'
    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      generationId: 1,
      groupProtocol: 'AssignerName',
      leaderId: memberId,
      memberId,
      members: [
        { memberId, groupInstanceId: 'group-instance-id', memberMetadata: Buffer.from(v2AssignerMetadataFixture.data) },
      ],
    })
    await expect(joinGroupResponseV5.parse(data)).resolves.toBeTruthy()
  })
})
