import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, float64, int16, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeClientQuotasEntity {
  entityType: string;
  entityName: string | null;
}

export interface DescribeClientQuotasValue {
  key: string;
  value: number;
}

export interface DescribeClientQuotasEntry {
  entity: DescribeClientQuotasEntity[];
  values: DescribeClientQuotasValue[];
}

export interface DescribeClientQuotasResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  entries: DescribeClientQuotasEntry[];
}

/**
 * DescribeClientQuotas Response (Version: 0) => throttle_time_ms error_code error_message [entries]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   entries => [entity] [values]
 *     entity => entity_type entity_name
 *       entity_type => STRING
 *       entity_name => NULLABLE_STRING
 *     values => key value
 *       key => STRING
 *       value => FLOAT64
 *
 * KIP-546. The wire's throttle_time_ms is client-side (KIP-219); exposed as `clientSideThrottleTime`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = object([field('entityType', string), field('entityName', nullableString)]);
const valueSchema = object([field('key', string), field('value', float64)]);
const entrySchema = object([field('entity', array(entitySchema)), field('values', array(valueSchema))]);
const restSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('entries', array(entrySchema)),
]);

export const describeClientQuotasResponseV0: ResponseDefinition<DescribeClientQuotasResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
