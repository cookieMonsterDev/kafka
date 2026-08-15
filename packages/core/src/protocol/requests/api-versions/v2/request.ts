import { defineRequest, object } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([])

export const apiVersionsRequestV2 = defineRequest({
  apiKey: API_KEYS.ApiVersions,
  apiVersion: 2,
  apiName: 'ApiVersions',
  schema: requestSchema,
})
