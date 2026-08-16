import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  float64,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeClientQuotasResponseV0Body } from '../v0/response';

export type DescribeClientQuotasResponseV1Body = DescribeClientQuotasResponseV0Body;

/**
 * DescribeClientQuotas Response (Version: 1) => throttle_time_ms error_code error_message [entries] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   entries => [entity] [values] TAG_BUFFER
 *     entity => entity_type entity_name TAG_BUFFER
 *       entity_type => COMPACT_STRING
 *       entity_name => COMPACT_NULLABLE_STRING
 *     values => key value TAG_BUFFER
 *       key => COMPACT_STRING
 *       value => FLOAT64
 *
 * Flexible form of v0 (KIP-482). Quota timing follows v0 (KIP-219). Response header v1's
 * trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = flexibleObject([field('entityType', compactString), field('entityName', compactNullableString)]);
const valueSchema = flexibleObject([field('key', compactString), field('value', float64)]);
const entrySchema = flexibleObject([
  field('entity', compactArray(entitySchema)),
  field('values', compactArray(valueSchema)),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('entries', compactArray(entrySchema)),
]);

export const describeClientQuotasResponseV1: ResponseDefinition<DescribeClientQuotasResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
