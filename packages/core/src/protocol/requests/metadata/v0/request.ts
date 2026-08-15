import { array, defineRequest, field, object, string } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

/**
 * Metadata Request (Version: 0) => [topics]
 *   topics => STRING
 */
const requestSchema = object([field('topics', array(string))])

export const metadataRequestV0 = defineRequest({
  apiKey: API_KEYS.Metadata,
  apiVersion: 0,
  apiName: 'Metadata',
  schema: requestSchema,
})
