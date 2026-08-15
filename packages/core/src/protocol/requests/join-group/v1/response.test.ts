import { describe, expect, it } from 'vitest'
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' }
import { joinGroupResponseV1 } from './response.js'

describe('protocol/requests/join-group/v1/response', () => {
  it('decodes a real fixture (identical wire format to v0)', async () => {
    const data = await joinGroupResponseV1.decode(Buffer.from(v1ResponseFixture.data))
    expect(data.errorCode).toBe(0)
    expect(data.members).toHaveLength(1)
    await expect(joinGroupResponseV1.parse(data)).resolves.toBeTruthy()
  })
})
