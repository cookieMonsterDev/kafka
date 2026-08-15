import { describe, expect, it } from 'vitest'
import v1ResponseFixture from '../fixtures/v1-response.json' with { type: 'json' }
import { heartbeatResponseV1 } from './response.js'

describe('protocol/requests/heartbeat/v1/response', () => {
  it('decodes a real fixture', async () => {
    const data = await heartbeatResponseV1.decode(Buffer.from(v1ResponseFixture.data))
    expect(data).toEqual({ throttleTime: 0, errorCode: 0 })
    await expect(heartbeatResponseV1.parse(data)).resolves.toBeTruthy()
  })
})
