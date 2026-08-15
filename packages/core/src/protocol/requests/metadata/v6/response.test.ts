import { describe, expect, it } from 'vitest'
import v5ResponseFixture from '../fixtures/v5-response.json' with { type: 'json' }
import { metadataResponseV6 } from './response.js'

describe('protocol/requests/metadata/v6/response', () => {
  it('decodes the v5 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const data = await metadataResponseV6.decode(Buffer.from(v5ResponseFixture.data))
    expect(data.throttleTime).toBe(0)
    expect(data.clientSideThrottleTime).toBe(0)
    expect(data.clusterId).toBe('wyOEk0m7Tn-08oGZjtVgEg')
    await expect(metadataResponseV6.parse(data)).resolves.toBeTruthy()
  })
})
