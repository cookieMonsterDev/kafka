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
  int8,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { DescribeAclsResponseV1Body } from '../v1/response';

export type DescribeAclsResponseV2Body = DescribeAclsResponseV1Body;

const aclSchema = flexibleObject([
  field('principal', compactString),
  field('host', compactString),
  field('operation', int8),
  field('permissionType', int8),
]);
const resourceSchema = flexibleObject([
  field('resourceType', int8),
  field('resourceName', compactString),
  field('resourcePatternType', int8),
  field('acls', compactArray(aclSchema)),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('resources', compactArray(resourceSchema)),
]);

/**
 * DescribeAcls Response (Version: 2) => throttle_time_ms error_code error_message [resources] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   resources => resource_type resource_name resource_pattern_type [acls] TAG_BUFFER
 *     resource_type => INT8
 *     resource_name => COMPACT_STRING
 *     resource_pattern_type => INT8
 *     acls => principal host operation permission_type TAG_BUFFER
 *       principal => COMPACT_STRING
 *       host => COMPACT_STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const describeAclsResponseV2: ResponseDefinition<DescribeAclsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
