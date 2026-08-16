import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { AlterClientQuotasResponseV0Body } from '../v0/response';

export type AlterClientQuotasResponseV1Body = AlterClientQuotasResponseV0Body;

/**
 * AlterClientQuotas Response (Version: 1) => throttle_time_ms [entries] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   entries => error_code error_message [entity] TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     entity => entity_type entity_name TAG_BUFFER
 *       entity_type => COMPACT_STRING
 *       entity_name => COMPACT_NULLABLE_STRING
 *
 * Flexible form of v0 (KIP-482). Quota timing follows v0 (KIP-219). Response header v1's
 * trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const entitySchema = flexibleObject([field('entityType', compactString), field('entityName', compactNullableString)]);
const entrySchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('entity', compactArray(entitySchema)),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('entries', compactArray(entrySchema))]);

export const alterClientQuotasResponseV1: ResponseDefinition<AlterClientQuotasResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const entryWithError = data.entries.find((entry) => failure(entry.errorCode));
    if (entryWithError) throw createErrorFromCode(entryWithError.errorCode);
    return data;
  },
};
