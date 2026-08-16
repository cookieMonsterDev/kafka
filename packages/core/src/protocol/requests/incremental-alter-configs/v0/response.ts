import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, int8, nullableString, object, string } from '../../../schema';

export interface IncrementalAlterConfigsResourceResult {
  errorCode: number;
  errorMessage: string | null;
  resourceType: number;
  resourceName: string;
}

export interface IncrementalAlterConfigsResponseV0Body {
  throttleTime: number;
  resources: IncrementalAlterConfigsResourceResult[];
}

/**
 * IncrementalAlterConfigs Response (Version: 0) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => error_code error_message resource_type resource_name
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const resourceSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resourceType', int8),
  field('resourceName', string),
]);
export const responseSchema = object([field('throttleTime', int32), field('resources', array(resourceSchema))]);

export const incrementalAlterConfigsResponseV0 = defineResponse<IncrementalAlterConfigsResponseV0Body>({
  schema: responseSchema,
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
});
