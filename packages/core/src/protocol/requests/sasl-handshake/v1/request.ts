import { defineRequest, field, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([field('mechanism', string)])

export const saslHandshakeRequestV1 = defineRequest({
  apiKey: API_KEYS.SaslHandshake,
  apiVersion: 1,
  apiName: 'SaslHandshake',
  schema: requestSchema,
})
