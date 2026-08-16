import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  defineResponse,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
} from '../../../schema';
import type { IncrementalAlterConfigsResponseV0Body } from '../v0/response';

export type IncrementalAlterConfigsResponseV1Body = IncrementalAlterConfigsResponseV0Body;

/**
 * IncrementalAlterConfigs Response (Version: 1) => throttle_time_ms [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *
 * Flexible-version API. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const resourceSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('resourceType', int8),
  field('resourceName', compactString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('resources', compactArray(resourceSchema)),
]);

export const incrementalAlterConfigsResponseV1 = defineResponse<IncrementalAlterConfigsResponseV1Body>({
  schema: responseSchema,
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
});
