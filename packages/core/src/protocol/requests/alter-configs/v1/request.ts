import { defineRequest } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';
import { requestSchema } from '../v0/request.js';

export {
  type AlterConfigsEntry,
  type AlterConfigsRequestV0Fields as AlterConfigsRequestV1Fields,
  type AlterConfigsResource,
} from '../v0/request.js';

/**
 * AlterConfigs Request (Version: 1) — wire format identical to v0; only the response's
 * throttling semantics change (KIP-219).
 */
export const alterConfigsRequestV1 = defineRequest({
  apiKey: API_KEYS.AlterConfigs,
  apiVersion: 1,
  apiName: 'AlterConfigs',
  schema: requestSchema,
});
