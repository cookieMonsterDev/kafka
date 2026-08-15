import { defineRequest, field, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([field('groupId', string), field('memberId', string)])

export const leaveGroupRequestV2 = defineRequest({
  apiKey: API_KEYS.LeaveGroup,
  apiVersion: 2,
  apiName: 'LeaveGroup',
  schema: requestSchema,
})
