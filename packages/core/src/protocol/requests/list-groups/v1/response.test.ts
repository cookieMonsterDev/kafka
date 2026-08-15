import { describe, expect, it } from 'vitest'
import { Encoder } from '../../../encoder.js'
import { listGroupsResponseV1 } from './response.js'

describe('protocol/requests/list-groups/v1/response', () => {
  it('decodes throttleTime ahead of error_code and groups', async () => {
    const wire = new Encoder().writeInt32(5).writeInt16(0).writeInt32(0).buffer
    const data = await listGroupsResponseV1.decode(wire)
    expect(data).toEqual({ throttleTime: 5, errorCode: 0, groups: [] })
    await expect(listGroupsResponseV1.parse(data)).resolves.toBeTruthy()
  })
})
