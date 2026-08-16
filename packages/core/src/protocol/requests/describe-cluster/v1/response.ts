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
import type { DescribeClusterBroker, DescribeClusterResponseV0Body } from '../v0/response';

export type { DescribeClusterBroker };

export interface DescribeClusterResponseV1Body extends DescribeClusterResponseV0Body {
  endpointType: number;
}

/**
 * DescribeCluster Response (Version: 1) => throttle_time_ms error_code error_message endpoint_type
 *                                          cluster_id controller_id [brokers] cluster_authorized_operations
 *                                          TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   endpoint_type => INT8
 *   cluster_id => COMPACT_STRING
 *   controller_id => INT32
 *   brokers => node_id host port rack TAG_BUFFER
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     rack => COMPACT_NULLABLE_STRING
 *   cluster_authorized_operations => INT32
 *
 * Adds `endpointType` (KIP-919). `MISMATCHED_ENDPOINT_TYPE` and `UNSUPPORTED_ENDPOINT_TYPE`
 * are valid top-level error codes. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const brokerSchema = flexibleObject([
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('rack', compactNullableString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('endpointType', int8),
  field('clusterId', compactString),
  field('controllerId', int32),
  field('brokers', compactArray(brokerSchema)),
  field('clusterAuthorizedOperations', int32),
]);

export const describeClusterResponseV1: ResponseDefinition<DescribeClusterResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
