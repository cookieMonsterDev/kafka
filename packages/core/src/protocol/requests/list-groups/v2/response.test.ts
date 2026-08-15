import { describe, expect, it } from 'vitest'
import { Encoder } from '../../../encoder.js'
import { listGroupsResponseV2 } from './response.js'

describe('protocol/requests/list-groups/v2/response', () => {
  it('decodes the v1 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const wire = new Encoder().writeInt32(5).writeInt16(0).writeInt32(0).buffer
    const data = await listGroupsResponseV2.decode(wire)
    expect(data).toEqual({ throttleTime: 0, clientSideThrottleTime: 5, errorCode: 0, groups: [] })
    await expect(listGroupsResponseV2.parse(data)).resolves.toBeTruthy()
  })
})
