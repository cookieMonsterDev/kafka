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
import type { DeleteAclsResponseV1Body } from '../v1/response';

export type DeleteAclsResponseV2Body = DeleteAclsResponseV1Body;

const matchingAclSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('resourceType', int8),
  field('resourceName', compactString),
  field('resourcePatternType', int8),
  field('principal', compactString),
  field('host', compactString),
  field('operation', int8),
  field('permissionType', int8),
]);
const filterResponseSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('matchingAcls', compactArray(matchingAclSchema)),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('filterResponses', compactArray(filterResponseSchema)),
]);

/**
 * DeleteAcls Response (Version: 2) => throttle_time_ms [filter_responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   filter_responses => error_code error_message [matching_acls] TAG_BUFFER
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     matching_acls => error_code error_message resource_type resource_name resource_pattern_type principal host operation permission_type TAG_BUFFER
 *       error_code => INT16
 *       error_message => COMPACT_NULLABLE_STRING
 *       resource_type => INT8
 *       resource_name => COMPACT_STRING
 *       resource_pattern_type => INT8
 *       principal => COMPACT_STRING
 *       host => COMPACT_STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * Flexible compact + tagged form of v1. Quota timing follows v1 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const deleteAclsResponseV2: ResponseDefinition<DeleteAclsResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const filterWithError = data.filterResponses.find(({ errorCode }) => failure(errorCode));
    if (filterWithError) throw createErrorFromCode(filterWithError.errorCode);

    for (const filterResponse of data.filterResponses) {
      const aclWithError = filterResponse.matchingAcls.find(({ errorCode }) => failure(errorCode));
      if (aclWithError) throw createErrorFromCode(aclWithError.errorCode);
    }

    return data;
  },
};
