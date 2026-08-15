import { describe, expect, it } from 'vitest'
import v3ResponseFixture from '../fixtures/v3-response.json' with { type: 'json' }
import { leaveGroupResponseV3 } from './response.js'

describe('protocol/requests/leave-group/v3/response', () => {
  it('decodes a real fixture, including the per-member batch', async () => {
    const data = await leaveGroupResponseV3.decode(Buffer.from(v3ResponseFixture.data))

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      members: [{ memberId: 'test-42962f68-e801-4cd8-b359-2d862ebb4d05', groupInstanceId: null, errorCode: 0 }],
    })
    await expect(leaveGroupResponseV3.parse(data)).resolves.toBeTruthy()
  })

  it('throws using the first member error code when a member reports failure', async () => {
    const data = {
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      members: [
        { memberId: 'ok', groupInstanceId: null, errorCode: 0 },
        { memberId: 'bad', groupInstanceId: null, errorCode: 35 },
      ],
    }
    await expect(leaveGroupResponseV3.parse(data)).rejects.toThrow(/version of API is not supported/)
  })
})
