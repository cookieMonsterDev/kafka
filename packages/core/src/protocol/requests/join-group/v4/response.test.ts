import { describe, expect, it } from 'vitest'
import { KafkaJSMemberIdRequired } from '../../../../errors.js'
import { Encoder } from '../../../encoder.js'
import { joinGroupResponseV4 } from './response.js'

function buildWire(errorCode: number): Buffer {
  return new Encoder()
    .writeInt32(0) // throttleTime
    .writeInt16(errorCode)
    .writeInt32(0) // generationId
    .writeString('proto')
    .writeString('leader')
    .writeString('member-1')
    .writeArray([]) // members
    .buffer
}

describe('protocol/requests/join-group/v4/response', () => {
  it('decodes cleanly and resolves on success', async () => {
    const data = await joinGroupResponseV4.decode(buildWire(0))
    await expect(joinGroupResponseV4.parse(data)).resolves.toBeTruthy()
  })

  it('throws KafkaJSMemberIdRequired carrying memberId on MEMBER_ID_REQUIRED', async () => {
    const data = await joinGroupResponseV4.decode(buildWire(79))
    const error: unknown = await joinGroupResponseV4.parse(data).catch((e: unknown) => e)
    expect(error).toBeInstanceOf(KafkaJSMemberIdRequired)
    expect((error as KafkaJSMemberIdRequired).memberId).toBe('member-1')
  })
})
