import { compactString, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * FindCoordinator Request (Version: 3) => key key_type TAG_BUFFER
 *   key => COMPACT_STRING
 *   key_type => INT8
 *
 * First flexible version (KIP-482). Same fields as v1/v2 with compact coordinatorKey.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 * This client keeps `apiName: 'GroupCoordinator'` (API key 10).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const requestSchema = flexibleObject([field('coordinatorKey', compactString), field('coordinatorType', int8)]);

export const findCoordinatorRequestV3 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 3,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
