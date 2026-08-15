import { describe, expect, it } from 'vitest'
import { SyncGroup } from './index.js'

describe('protocol/requests/sync-group', () => {
  it('implements versions 0 through 3', () => {
    expect(SyncGroup.versions).toEqual([0, 1, 2, 3])
  })

  it('defaults groupInstanceId to null on v3 when omitted', () => {
    const { request } = SyncGroup.protocol({ version: 3 })({
      groupId: 'g',
      generationId: 1,
      memberId: 'm',
      groupAssignment: [],
    })
    expect(request.apiVersion).toBe(3)
  })
})
