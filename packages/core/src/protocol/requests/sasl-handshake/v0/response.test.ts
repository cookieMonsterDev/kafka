import { describe, expect, it } from 'vitest'
import { Encoder } from '../../../encoder.js'
import { saslHandshakeResponseV0 } from './response.js'

function unsupportedVersionResponse(): Buffer {
  return Buffer.from([0, 35, 0, 0, 0, 0])
}

describe('protocol/requests/sasl-handshake/v0/response', () => {
  it('round-trips the enabled mechanisms list', async () => {
    const wire = new Encoder().writeInt16(0).writeArray(['PLAIN', 'SCRAM-SHA-256'], 'string').buffer
    const data = await saslHandshakeResponseV0.decode(wire)

    expect(data).toEqual({ errorCode: 0, enabledMechanisms: ['PLAIN', 'SCRAM-SHA-256'] })
    await expect(saslHandshakeResponseV0.parse(data)).resolves.toBeTruthy()
  })

  it('throws a KafkaJSProtocolError if the api is not supported', async () => {
    const data = await saslHandshakeResponseV0.decode(unsupportedVersionResponse())
    await expect(saslHandshakeResponseV0.parse(data)).rejects.toThrow(/version of API is not supported/)
  })
})
