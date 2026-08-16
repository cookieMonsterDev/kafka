import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int32, object } from '../../../schema';
import { groupSchema } from '../v0/response';

/**
 * DescribeGroups Response (Version: 1) => throttle_time_ms [groups]
 *   throttle_time_ms => INT32
 *   groups => error_code group_id state protocol_type protocol [members]
 */
const bodySchema = object([field('throttleTime', int32), field('groups', array(groupSchema))]);

export const describeGroupsResponseV1 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    const groupWithError = data.groups.find((group) => failure(group.errorCode));
    if (groupWithError) throw createErrorFromCode(groupWithError.errorCode);
    return data;
  },
});
