import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, defineResponse, field, int16, object, string } from '../../../schema.js';

/**
 * ListGroups Response (Version: 0) => error_code [groups]
 *   error_code => INT16
 *   groups => group_id protocol_type
 *     group_id => STRING
 *     protocol_type => STRING
 */
export const groupSchema = object([field('groupId', string), field('protocolType', string)]);
const bodySchema = object([field('errorCode', int16), field('groups', array(groupSchema))]);

export const listGroupsResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
});
