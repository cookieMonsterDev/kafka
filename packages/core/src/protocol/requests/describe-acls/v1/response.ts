import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import { array, field, int16, int8, nullableString, object, string } from '../../../schema.js';
import type { ResponseDefinition } from '../../../schema.js';

/**
 * On quota violation, brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 * Introduces a resource pattern type field.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-290%3A+Support+for+Prefixed+ACLs
 *
 * DescribeAcls Response (Version: 1) => throttle_time_ms error_code error_message [resources]
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   resources => resource_type resource_name resource_pattern_type [acls]
 *     resource_type => INT8
 *     resource_name => STRING
 *     resource_pattern_type => INT8
 *     acls => principal host operation permission_type
 *       principal => STRING
 *       host => STRING
 *       operation => INT8
 *       permission_type => INT8
 *
 * The wire's throttle_time_ms is client-side (KIP-219); the raw value is exposed as
 * `clientSideThrottleTime` and `throttleTime` is always 0.
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
  field('resourcePatternType', int8),
  field('acls', array(aclSchema)),
]);
const restSchema = object([
  field('errorCode', int16),
  field('errorMessage', nullableString),
  field('resources', array(resourceSchema)),
]);

export interface DescribeAclsResponseV1Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  resources: {
    resourceType: number;
    resourceName: string;
    resourcePatternType: number;
    acls: { principal: string; host: string; operation: number; permissionType: number }[];
  }[];
}

export const describeAclsResponseV1: ResponseDefinition<DescribeAclsResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const clientSideThrottleTime = decoder.readInt32();
    const rest = restSchema.read(decoder);
    return { throttleTime: 0, clientSideThrottleTime, ...rest };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
