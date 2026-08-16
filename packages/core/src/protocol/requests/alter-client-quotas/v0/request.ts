import { array, boolean, defineRequest, field, float64, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AlterClientQuotasEntity {
  entityType: string;
  entityName: string | null;
}

export interface AlterClientQuotasOp {
  key: string;
  value: number;
  remove: boolean;
}

export interface AlterClientQuotasEntry {
  entity: AlterClientQuotasEntity[];
  ops: AlterClientQuotasOp[];
}

export interface AlterClientQuotasRequestV0Fields {
  entries: AlterClientQuotasEntry[];
  validateOnly: boolean;
}

/**
 * AlterClientQuotas Request (Version: 0) => [entries] validate_only
 *   entries => [entity] [ops]
 *     entity => entity_type entity_name
 *       entity_type => STRING
 *       entity_name => NULLABLE_STRING
 *     ops => key value remove
 *       key => STRING
 *       value => FLOAT64
 *       remove => BOOLEAN
 *   validate_only => BOOLEAN
 *
 * KIP-546. `remove: true` deletes the quota key; `value` is ignored on the broker then.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = object([field('entityType', string), field('entityName', nullableString)]);
const opSchema = object([field('key', string), field('value', float64), field('remove', boolean)]);
const entrySchema = object([field('entity', array(entitySchema)), field('ops', array(opSchema))]);
export const requestSchema = object([field('entries', array(entrySchema)), field('validateOnly', boolean)]);

export const alterClientQuotasRequestV0 = defineRequest({
  apiKey: API_KEYS.AlterClientQuotas,
  apiVersion: 0,
  apiName: 'AlterClientQuotas',
  schema: requestSchema,
});
