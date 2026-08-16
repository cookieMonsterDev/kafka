import { Decoder } from '../../../decoder';
import { RESOURCE_PATTERN_TYPES } from '../../../enums/resource-pattern-types';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, int8, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

/**
 * DeleteAcls Response (Version: 0) => throttle_time_ms [filter_responses]
 *   throttle_time_ms => INT32
 *   filter_responses => error_code error_message [matching_acls]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     matching_acls => error_code error_message resource_type resource_name principal host operation permission_type
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 *       resource_type => INT8
 *       resource_name => STRING
 *       principal => STRING
 *       host => STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * `resourcePatternType` is not on the wire; decoded matching ACLs default to LITERAL.
 */
const matchingAclSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resourceType', int8),
  field('resourceName', string),
  field('principal', string),
  field('host', string),
  field('operation', int8),
  field('permissionType', int8),
]);
const filterResponseSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('matchingAcls', array(matchingAclSchema)),
]);
const restSchema = object([field('filterResponses', array(filterResponseSchema))]);

export interface DeleteAclsResponseV0Body {
  throttleTime: number;
  filterResponses: {
    errorCode: number;
    errorMessage: string | null;
    matchingAcls: {
      errorCode: number;
      errorMessage: string | null;
      resourceType: number;
      resourceName: string;
      resourcePatternType: number;
      principal: string;
      host: string;
      operation: number;
      permissionType: number;
    }[];
  }[];
}

export const deleteAclsResponseV0: ResponseDefinition<DeleteAclsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return {
      throttleTime,
      filterResponses: rest.filterResponses.map((filterResponse) => ({
        ...filterResponse,
        matchingAcls: filterResponse.matchingAcls.map((acl) => ({
          ...acl,
          resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        })),
      })),
    };
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
