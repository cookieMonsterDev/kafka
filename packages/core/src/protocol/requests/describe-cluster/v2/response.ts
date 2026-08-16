import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  boolean,
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
import type { DescribeClusterBroker as DescribeClusterBrokerV0 } from '../v0/response';
import type { DescribeClusterResponseV1Body } from '../v1/response';

export interface DescribeClusterBroker extends DescribeClusterBrokerV0 {
  isFenced: boolean;
}

export type DescribeClusterResponseV2Body = Omit<DescribeClusterResponseV1Body, 'brokers'> & {
  brokers: DescribeClusterBroker[];
};

/**
 * DescribeCluster Response (Version: 2) => throttle_time_ms error_code error_message endpoint_type
 *                                          cluster_id controller_id [brokers] cluster_authorized_operations
 *                                          TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   endpoint_type => INT8
 *   cluster_id => COMPACT_STRING
 *   controller_id => INT32
 *   brokers => node_id host port rack is_fenced TAG_BUFFER
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     rack => COMPACT_NULLABLE_STRING
 *     is_fenced => BOOLEAN
 *   cluster_authorized_operations => INT32
 *
 * Adds `isFenced` on each broker (KIP-1073). Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const brokerSchema = flexibleObject([
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('rack', compactNullableString),
  field('isFenced', boolean),
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

export const describeClusterResponseV2: ResponseDefinition<DescribeClusterResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
