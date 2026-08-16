import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { array, field, int16, int8, nullableString, object, string } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 * Introduces a resource pattern type field.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs
 *
 * DeleteAcls Response (Version: 1) => throttle_time_ms [filter_responses]
 *   throttle_time_ms => INT32
 *   filter_responses => error_code error_message [matching_acls]
 *     error_code => INT16
 *     error_message => NULLABLE_STRING
 *     matching_acls => error_code error_message resource_type resource_name resource_pattern_type principal host operation permission_type
 *       error_code => INT16
 *       error_message => NULLABLE_STRING
 *       resource_type => INT8
 *       resource_name => STRING
 *       resource_pattern_type => INT8
 *       principal => STRING
 *       host => STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * The wire's throttle_time_ms is client-side (KIP-219); the raw value is exposed as
 * `clientSideThrottleTime` and `throttleTime` is always 0.
 */
const matchingAclSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resourceType', int8),
  field('resourceName', string),
  field('resourcePatternType', int8),
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

export interface DeleteAclsResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
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

export const deleteAclsResponseV1: ResponseDefinition<DeleteAclsResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
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
