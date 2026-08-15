import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js'
import { defineResponse, field, int16, object } from '../../../schema.js'

/**
 * LeaveGroup Response (Version: 0) => error_code
 *   error_code => INT16
 */
const bodySchema = object([field('errorCode', int16)])

export const leaveGroupResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode)
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode)
    return data
  },
})
