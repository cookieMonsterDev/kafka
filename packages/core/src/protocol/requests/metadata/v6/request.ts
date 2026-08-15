import { boolean, defineRequest, field, nullableArray, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([field('topics', nullableArray(string)), field('allowAutoTopicCreation', boolean)])

export const metadataRequestV6 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 6,
  apiName: 'Metadata',
  schema: requestSchema,
})
