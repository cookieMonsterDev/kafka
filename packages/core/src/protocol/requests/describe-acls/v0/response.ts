import { Decoder } from '../../../decoder';
import { RESOURCE_PATTERN_TYPES } from '../../../enums/resource-pattern-types';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, int8, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

/**
 * DescribeAcls Response (Version: 0) => throttle_time_ms error_code error_message [resources]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   resources => resource_type resource_name [acls]
 *     resource_type => INT8
 *     resource_name => STRING
 *     acls => principal host operation permission_type
 *       principal => STRING
 *       host => STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * `resourcePatternType` is not on the wire; decoded resources default to LITERAL so
 * callers can share the v1 result shape.
 */
const aclSchema = object([
  field('principal', string),
  field('host', string),
  field('operation', int8),
  field('permissionType', int8),
]);
const resourceSchema = object([
  field('resourceType', int8),
  field('resourceName', string),
  field('acls', array(aclSchema)),
]);
const restSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resources', array(resourceSchema)),
]);

export interface DescribeAclsResponseV0Body {
  throttleTime: number;
  errorCode: number;
  errorMessage: string | null;
  resources: {
    resourceType: number;
    resourceName: string;
    resourcePatternType: number;
    acls: { principal: string; host: string; operation: number; permissionType: number }[];
  }[];
}

export const describeAclsResponseV0: ResponseDefinition<DescribeAclsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return {
      throttleTime,
      ...rest,
      resources: rest.resources.map((resource) => ({
        ...resource,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
      })),
    };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
