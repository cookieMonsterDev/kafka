import { createErrorFromCode, failure } from '../../../error-codes';
import { array, defineResponse, field, int16, int32, int8, nullableString, object, string } from '../../../schema';

export interface AlterConfigsResourceResult {
  errorCode: number;
  errorMessage: string | null;
  resourceType: number;
  resourceName: string;
}

export interface AlterConfigsResponseV0Body {
  throttleTime: number;
  resources: AlterConfigsResourceResult[];
}

/**
 * AlterConfigs Response (Version: 0) => throttle_time_ms [resources]
 *   throttle_time_ms => INT32
 *   resources => error_code error_message resource_type resource_name
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     resource_type => INT8
 *     resource_name => STRING
 */
const resourceSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resourceType', int8),
  field('resourceName', string),
]);
const bodySchema = object([field('throttleTime', int32), field('resources', array(resourceSchema))]);

export const alterConfigsResponseV0 = defineResponse<AlterConfigsResponseV0Body>({
  schema: bodySchema,
  parse: async (data) => {
    const resourceWithError = data.resources.find((resource) => failure(resource.errorCode));
    if (resourceWithError) throw createErrorFromCode(resourceWithError.errorCode);
    return data;
  },
});
