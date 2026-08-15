import { describe, expect, it } from 'vitest'
import scram256FirstRequestResponseV1Fixture from '../fixtures/scram256-first-request-response-v1.json' with { type: 'json' }
import v1ResponsePlainFixture from '../fixtures/v1-response-plain.json' with { type: 'json' }
import v1ResponseScram256Fixture from '../fixtures/v1-response-scram256.json' with { type: 'json' }
import { saslAuthenticateResponseV1 } from './response.js'

describe('protocol/requests/sasl-authenticate/v1/response', () => {
  it('decodes a real PLAIN fixture, including sessionLifetimeMs as bigint', async () => {
    const data = await saslAuthenticateResponseV1.decode(Buffer.from(v1ResponsePlainFixture.data))
    expect(data).toEqual({
      authBytes: Buffer.from([0, 0, 0, 0]),
      errorCode: 0,
      errorMessage: null,
      sessionLifetimeMs: 360000n,
    })
    await expect(saslAuthenticateResponseV1.parse(data)).resolves.toBeTruthy()
  })

  it('decodes a real SCRAM-SHA-256 fixture', async () => {
    const data = await saslAuthenticateResponseV1.decode(Buffer.from(v1ResponseScram256Fixture.data))
    expect(data).toEqual({
      authBytes: Buffer.from(scram256FirstRequestResponseV1Fixture.data),
      errorCode: 0,
      errorMessage: null,
      sessionLifetimeMs: 360000n,
    })
    await expect(saslAuthenticateResponseV1.parse(data)).resolves.toBeTruthy()
  })
})
