import { defineRequest, object } from '../../../schema.js'
import { API_KEYS } from '../../api-keys.js'

const requestSchema = object([])

export const listGroupsRequestV1 = defineRequest({
  apiKey: API_KEYS.ListGroups,
  apiVersion: 1,
  apiName: 'ListGroups',
  schema: requestSchema,
})
