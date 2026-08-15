import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes.js'
import { defineResponse, field, int16, int32, object, string } from '../../../schema.js'

/**
 * FindCoordinator Response (Version: 0) => error_code coordinator
 *   error_code => INT16
 *   coordinator => node_id host port
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 */
const coordinatorSchema = object([field('nodeId', int32), field('host', string), field('port', int32)])
const bodySchema = object([field('errorCode', int16), field('coordinator', coordinatorSchema)])

export const findCoordinatorResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    failIfVersionNotSupported(data.errorCode)
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode)
    return data
  },
})
