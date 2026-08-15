import { boolean, defineRequest, field, nullableArray, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * Metadata Request (Version: 4) => [topics] allow_auto_topic_creation
 *   topics => STRING
 *   allow_auto_topic_creation => BOOLEAN
 */
const requestSchema = object([field('topics', nullableArray(string)), field('allowAutoTopicCreation', boolean)])

export const metadataRequestV4 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 4,
  apiName: 'Metadata',
  schema: requestSchema,
})
