import { compactNullableString, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * Heartbeat Request (Version: 4) => group_id generation_id member_id group_instance_id TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   generation_id => INT32
 *   member_id => COMPACT_STRING
 *   group_instance_id => COMPACT_NULLABLE_STRING
 *
 * First flexible version (KIP-482). Same fields as v3. Request header v2's trailing TAG_BUFFER is
 * written by `createRequest`, not here.
 */
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('groupGenerationId', int32),
  field('memberId', compactString),
  field('groupInstanceId', compactNullableString),
]);

export const heartbeatRequestV4 = defineRequest({
  apiKey: API_KEYS.Heartbeat,
  apiVersion: 4,
  apiName: 'Heartbeat',
  schema: requestSchema,
});
