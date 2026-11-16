import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeClusterRequestV0 } from './v0/request';
import { describeClusterResponseV0 } from './v0/response';
import { describeClusterRequestV1 } from './v1/request';
import { describeClusterResponseV1 } from './v1/response';
import { describeClusterRequestV2 } from './v2/request';
import { describeClusterResponseV2 } from './v2/response';

export type { DescribeClusterRequestV0Fields } from './v0/request';
export type { DescribeClusterBroker, DescribeClusterResponseV0Body } from './v0/response';
export type { DescribeClusterRequestV1Fields } from './v1/request';
export type { DescribeClusterResponseV1Body } from './v1/response';
export type { DescribeClusterRequestV2Fields } from './v2/request';
export type { DescribeClusterBroker as DescribeClusterBrokerV2, DescribeClusterResponseV2Body } from './v2/response';

/** 1=brokers, 2=controllers (KIP-919). */
const DEFAULT_ENDPOINT_TYPE = 1;

export interface DescribeClusterOptions {
  includeClusterAuthorizedOperations?: boolean;
  /** 1=brokers, 2=controllers. Used on v1+. Defaults to brokers. */
  endpointType?: number;
  /** Used on v2+. Defaults to false. */
  includeFencedBrokers?: boolean;
}

const VERSIONS = {
  0: (options) => ({
    request: describeClusterRequestV0({
      includeClusterAuthorizedOperations: options.includeClusterAuthorizedOperations ?? false,
    }),
    response: describeClusterResponseV0,
  }),
  1: (options) => ({
    request: describeClusterRequestV1({
      includeClusterAuthorizedOperations: options.includeClusterAuthorizedOperations ?? false,
      endpointType: options.endpointType ?? DEFAULT_ENDPOINT_TYPE,
    }),
    response: describeClusterResponseV1,
  }),
  2: (options) => ({
    request: describeClusterRequestV2({
      includeClusterAuthorizedOperations: options.includeClusterAuthorizedOperations ?? false,
      endpointType: options.endpointType ?? DEFAULT_ENDPOINT_TYPE,
      includeFencedBrokers: options.includeFencedBrokers ?? false,
    }),
    response: describeClusterResponseV2,
  }),
} satisfies VersionFactories<DescribeClusterOptions>;

export const DescribeCluster = defineRequestFamily(KAFKA_APIS.DescribeCluster, VERSIONS);
