import { describe, expect, it } from 'vitest'
import { Heartbeat } from './index.js'

describe('protocol/requests/heartbeat', () => {
  it('implements versions 0 through 3', () => {
    expect(Heartbeat.versions).toEqual([0, 1, 2, 3])
  })

  it('defaults groupInstanceId to null on v3 when omitted', async () => {
    const { request } = Heartbeat.protocol({ version: 3 })({
      groupId: 'group',
      groupGenerationId: 1,
      memberId: 'member',
    })
    const encoder = await request.encode()
    expect(encoder.buffer.subarray(-2)).toEqual(Buffer.from([0xff, 0xff]))
  })
})
