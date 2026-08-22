import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeQuorumOptions>>> = {
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
};

export const DescribeQuorum: RequestFamily<DescribeQuorumOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeQuorum protocol for version ${version}`);
    return factory;
  },
});
