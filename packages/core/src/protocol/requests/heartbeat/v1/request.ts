import { defineRequest, field, int32, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([
  field('groupId', string),
  field('groupGenerationId', int32),
  field('memberId', string),
])

export const heartbeatRequestV1 = defineRequest({
  apiKey: API_KEYS.Heartbeat,
  apiVersion: 1,
  apiName: 'Heartbeat',
  schema: requestSchema,
})
