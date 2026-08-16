import { array, boolean, defineRequest, field, int8, nullableString, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface DescribeClientQuotasComponent {
  entityType: string;
  matchType: number;
  match: string | null;
}

export interface DescribeClientQuotasRequestV0Fields {
  components: DescribeClientQuotasComponent[];
  strict: boolean;
}

/**
 * DescribeClientQuotas Request (Version: 0) => [components] strict
 *   components => entity_type match_type match
 *     entity_type => STRING
 *     match_type => INT8
 *     match => NULLABLE_STRING
 *   strict => BOOLEAN
 *
 * KIP-546 client quota filters. match_type: 0 exact, 1 default, 2 any specified name.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const componentSchema = object([field('entityType', string), field('matchType', int8), field('match', nullableString)]);
export const requestSchema = object([field('components', array(componentSchema)), field('strict', boolean)]);

export const describeClientQuotasRequestV0 = defineRequest({
  apiKey: API_KEYS.DescribeClientQuotas,
  apiVersion: 0,
  apiName: 'DescribeClientQuotas',
  schema: requestSchema,
});
