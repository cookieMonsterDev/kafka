import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import type { DescribeQuorumTopicInput } from './v0/request';
import { describeQuorumRequestV0 } from './v0/request';
import { describeQuorumResponseV0 } from './v0/response';
import { describeQuorumRequestV1 } from './v1/request';
import { describeQuorumResponseV1 } from './v1/response';
import { describeQuorumRequestV2 } from './v2/request';
import { describeQuorumResponseV2 } from './v2/response';

export type {
  DescribeQuorumPartitionInput,
  DescribeQuorumRequestV0Fields,
  DescribeQuorumTopicInput,
} from './v0/request';
export type {
  DescribeQuorumPartitionData,
  DescribeQuorumReplicaState,
  DescribeQuorumResponseV0Body,
  DescribeQuorumTopicData,
} from './v0/response';
export type {
  DescribeQuorumPartitionDataV1,
  DescribeQuorumReplicaStateV1,
  DescribeQuorumResponseV1Body,
  DescribeQuorumTopicDataV1,
} from './v1/response';
export type {
  DescribeQuorumListenerV2,
  DescribeQuorumNodeV2,
  DescribeQuorumPartitionDataV2,
  DescribeQuorumReplicaStateV2,
  DescribeQuorumResponseV2Body,
  DescribeQuorumTopicDataV2,
} from './v2/response';

export type DescribeQuorumResponseBody =
  | import('./v0/response').DescribeQuorumResponseV0Body
  | import('./v1/response').DescribeQuorumResponseV1Body
  | import('./v2/response').DescribeQuorumResponseV2Body;

export interface DescribeQuorumOptions {
  topics?: DescribeQuorumTopicInput[];
}

const VERSIONS = {
  0: (options) => ({
    request: describeQuorumRequestV0({ topics: options.topics ?? [] }),
    response: describeQuorumResponseV0,
  }),
  1: (options) => ({
    request: describeQuorumRequestV1({ topics: options.topics ?? [] }),
    response: describeQuorumResponseV1,
  }),
  2: (options) => ({
    request: describeQuorumRequestV2({ topics: options.topics ?? [] }),
    response: describeQuorumResponseV2,
  }),
} satisfies VersionFactories<DescribeQuorumOptions>;

export const DescribeQuorum = defineRequestFamily(KAFKA_APIS.DescribeQuorum, VERSIONS);
