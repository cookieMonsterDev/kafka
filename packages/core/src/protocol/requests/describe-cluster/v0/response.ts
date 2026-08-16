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
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeClusterBroker {
  nodeId: number;
  host: string;
  port: number;
  rack: string | null;
}

export interface DescribeClusterResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  clusterId: string;
  controllerId: number;
  brokers: DescribeClusterBroker[];
  clusterAuthorizedOperations: number;
}

/**
 * DescribeCluster Response (Version: 0) => throttle_time_ms error_code error_message cluster_id
 *                                          controller_id [brokers] cluster_authorized_operations TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   cluster_id => COMPACT_STRING
 *   controller_id => INT32
 *   brokers => node_id host port rack TAG_BUFFER
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     rack => COMPACT_NULLABLE_STRING
 *   cluster_authorized_operations => INT32
 *
 * Flexible from v0. Response header v1's trailing TAG_BUFFER is skipped by `Connection`
 * before `decode()` runs. Quota timing follows KIP-219: the decoded throttle is exposed as
 * `clientSideThrottleTime`.
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
  field('clusterId', compactString),
  field('controllerId', int32),
  field('brokers', compactArray(brokerSchema)),
  field('clusterAuthorizedOperations', int32),
]);

export const describeClusterResponseV0: ResponseDefinition<DescribeClusterResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
