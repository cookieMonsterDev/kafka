import { compactArray, compactString, defineRequest, field, flexibleObject, int8 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * FindCoordinator Request (Version: 4) => key_type [coordinator_keys] TAG_BUFFER
 *   key_type => INT8
 *   coordinator_keys => COMPACT_STRING
 *
 * Looks up the coordinators of several keys of one type in one request (KIP-699).
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const requestSchema = flexibleObject([
  field('coordinatorType', int8),
  field('coordinatorKeys', compactArray(compactString)),
]);

export const findCoordinatorRequestV4 = defineRequest({
  apiKey: API_KEYS.GroupCoordinator,
  apiVersion: 4,
  apiName: 'GroupCoordinator',
  schema: requestSchema,
});
