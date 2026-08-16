import {
  boolean,
  compactArray,
  compactNullableString,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  float64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

export {
  type AlterClientQuotasEntry,
  type AlterClientQuotasEntity,
  type AlterClientQuotasOp,
  type AlterClientQuotasRequestV0Fields as AlterClientQuotasRequestV1Fields,
} from '../v0/request';

/**
 * AlterClientQuotas Request (Version: 1) => [entries] validate_only TAG_BUFFER
 *   entries => [entity] [ops] TAG_BUFFER
 *     entity => entity_type entity_name TAG_BUFFER
 *       entity_type => COMPACT_STRING
 *       entity_name => COMPACT_NULLABLE_STRING
 *     ops => key value remove TAG_BUFFER
 *       key => COMPACT_STRING
 *       value => FLOAT64
 *       remove => BOOLEAN
 *   validate_only => BOOLEAN
 *
 * Flexible form of v0 (KIP-482). Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = flexibleObject([field('entityType', compactString), field('entityName', compactNullableString)]);
const opSchema = flexibleObject([field('key', compactString), field('value', float64), field('remove', boolean)]);
const entrySchema = flexibleObject([field('entity', compactArray(entitySchema)), field('ops', compactArray(opSchema))]);
export const requestSchema = flexibleObject([
  field('entries', compactArray(entrySchema)),
  field('validateOnly', boolean),
]);

export const alterClientQuotasRequestV1 = defineRequest({
  apiKey: API_KEYS.AlterClientQuotas,
  apiVersion: 1,
  apiName: 'AlterClientQuotas',
  schema: requestSchema,
});
