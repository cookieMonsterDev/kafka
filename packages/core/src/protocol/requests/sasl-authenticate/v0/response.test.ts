import { describe, expect, it } from 'vitest'
import scram256FirstRequestResponseFixture from '../fixtures/scram256-first-request-response.json' with { type: 'json' }
import v0ResponsePlainFixture from '../fixtures/v0-response-plain.json' with { type: 'json' }
import v0ResponseScram256Fixture from '../fixtures/v0-response-scram256.json' with { type: 'json' }
import { saslAuthenticateResponseV0 } from './response.js'

describe('protocol/requests/sasl-authenticate/v0/response', () => {
  it('decodes a real PLAIN fixture, re-framing authBytes with a fresh length prefix', async () => {
    const data = await saslAuthenticateResponseV0.decode(Buffer.from(v0ResponsePlainFixture.data))
    expect(data).toEqual({ authBytes: Buffer.from([0, 0, 0, 0]), errorCode: 0, errorMessage: null })
    await expect(saslAuthenticateResponseV0.parse(data)).resolves.toBeTruthy()
  })

  it('decodes a real SCRAM-SHA-256 fixture', async () => {
    const data = await saslAuthenticateResponseV0.decode(Buffer.from(v0ResponseScram256Fixture.data))
    expect(data).toEqual({
      authBytes: Buffer.from(scram256FirstRequestResponseFixture.data),
      errorCode: 0,
      errorMessage: null,
    })
    await expect(saslAuthenticateResponseV0.parse(data)).resolves.toBeTruthy()
  })

  it('uses the custom error message on SASL_AUTHENTICATION_FAILED', async () => {
    const data = { errorCode: 58, errorMessage: 'Auth failed', authBytes: Buffer.alloc(0) }
    await expect(saslAuthenticateResponseV0.parse(data)).rejects.toThrow(/Auth failed/)
  })
})
