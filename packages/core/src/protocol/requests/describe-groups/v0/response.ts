import { createErrorFromCode, failure } from '../../../error-codes.js'
import { array, bytes, defineResponse, field, int16, object, string } from '../../../schema.js'

/**
 * DescribeGroups Response (Version: 0) => [groups]
 *   groups => error_code group_id state protocol_type protocol [members]
 *     error_code => INT16
 *     group_id => STRING
 *     state => STRING
 *     protocol_type => STRING
 *     protocol => STRING
 *     members => member_id client_id client_host member_metadata member_assignment
 *       member_id => STRING
 *       client_id => STRING
 *       client_host => STRING
 *       member_metadata => BYTES
 *       member_assignment => BYTES
 */
export const memberSchema = object([
  field('memberId', string),
  field('clientId', string),
  field('clientHost', string),
  field('memberMetadata', bytes),
  field('memberAssignment', bytes),
])
export const groupSchema = object([
  field('errorCode', int16),
  field('groupId', string),
  field('state', string),
  field('protocolType', string),
  field('protocol', string),
  field('members', array(memberSchema)),
])
const bodySchema = object([field('groups', array(groupSchema))])

export const describeGroupsResponseV0 = defineResponse({
  schema: bodySchema,
  parse: async (data) => {
    const groupWithError = data.groups.find((group) => failure(group.errorCode))
    if (groupWithError) throw createErrorFromCode(groupWithError.errorCode)
    return data
  },
})
