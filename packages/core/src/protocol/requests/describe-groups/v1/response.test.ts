import { describe, expect, it } from 'vitest'
import v1MemberAssignmentFixture from '../fixtures/v1-member-assignment.json' with { type: 'json' }
import v1MemberMetadataFixture from '../fixtures/v1-member-metadata.json' with { type: 'json' }
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' }
import { describeGroupsResponseV1 } from './response.js'

describe('protocol/requests/describe-groups/v1/response', () => {
  it('decodes a real fixture, including throttleTime', async () => {
    const data = await describeGroupsResponseV1.decode(Buffer.from(v1ResponseFixture.data))

    expect(data).toEqual({
      throttleTime: 0,
      groups: [
        {
          errorCode: 0,
          groupId: 'consumer-group-id-4de0aa10ef94403a397d-53384-d2fee969-1446-4166-bc8e-c88e8daffdfe',
          state: 'Stable',
          protocolType: 'consumer',
          protocol: 'RoundRobinAssigner',
          members: [
            {
              memberId:
                'test-6ee008af511cbf89b897-53384-55bf525a-2ff5-49ef-8853-5fdf400a9c61-dbdee491-9f08-49d7-aa41-080b89bc69a8',
              clientId: 'test-6ee008af511cbf89b897-53384-55bf525a-2ff5-49ef-8853-5fdf400a9c61',
              clientHost: '/172.19.0.1',
              memberMetadata: Buffer.from(v1MemberMetadataFixture.data),
              memberAssignment: Buffer.from(v1MemberAssignmentFixture.data),
            },
          ],
        },
      ],
    })
    await expect(describeGroupsResponseV1.parse(data)).resolves.toBeTruthy()
  })
})
