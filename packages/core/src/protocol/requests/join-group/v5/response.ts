import { Decoder } from '../../../decoder.js'
import { KafkaJSMemberIdRequired } from '../../../../errors.js'
import { createErrorFromCode, ERROR_CODES, failIfVersionNotSupported, failure } from '../../../error-codes.js'
import { array, bytes, field, int16, int32, nullableString, object, string } from '../../../schema.js'
import type { ResponseDefinition } from '../../../schema.js'

const MEMBER_ID_REQUIRED_ERROR_CODE = ERROR_CODES.find((e) => e.type === 'MEMBER_ID_REQUIRED')?.code

/**
 * JoinGroup Response (Version: 5) => throttle_time_ms error_code generation_id group_protocol leader_id member_id [members]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   generation_id => INT32
 *   group_protocol => STRING
 *   leader_id => STRING
 *   member_id => STRING
 *   members => member_id group_instance_id member_metadata
 *     member_id => STRING
 *     group_instance_id => NULLABLE_STRING
 *     member_metadata => BYTES
 *
 * As in v3+ (KIP-219), the wire's throttle_time_ms is client-side; the raw value is exposed as
 * `clientSideThrottleTime` and `throttleTime` is always 0.
 */
const memberSchema = object([
  field('memberId', string),
  field('groupInstanceId', nullableString),
  field('memberMetadata', bytes),
])
const restSchema = object([
  field('errorCode', int16),
  field('generationId', int32),
  field('groupProtocol', string),
  field('leaderId', string),
  field('memberId', string),
  field('members', array(memberSchema)),
])

export interface JoinGroupResponseV5Body {
  throttleTime: number
  clientSideThrottleTime: number
  errorCode: number
  generationId: number
  groupProtocol: string
  leaderId: string
  memberId: string
  members: { memberId: string; groupInstanceId: string | null; memberMetadata: Buffer }[]
}

export const joinGroupResponseV5: ResponseDefinition<JoinGroupResponseV5Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData)
    const clientSideThrottleTime = decoder.readInt32()
    const rest = restSchema.read(decoder)
    return { throttleTime: 0, clientSideThrottleTime, ...rest }
  },
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode)
    if (failure(data.errorCode)) {
      if (data.errorCode === MEMBER_ID_REQUIRED_ERROR_CODE) {
        throw new KafkaJSMemberIdRequired(createErrorFromCode(data.errorCode), { memberId: data.memberId })
      }
      throw createErrorFromCode(data.errorCode)
    }
    return data
  },
}
