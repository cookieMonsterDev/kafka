import { describe, expect, it } from 'vitest'
import { Encoder } from '../../../encoder.js'
import { deleteGroupsRequestV0 } from './request.js'

describe('protocol/requests/delete-groups/v0/request', () => {
  it('encodes an array of group ids', async () => {
    const definition = deleteGroupsRequestV0({ groupIds: ['g1', 'g2'] })
    const encoder = await definition.encode()
    expect(encoder.buffer).toEqual(new Encoder().writeArray(['g1', 'g2']).buffer)
  })
})
