import type { ProtocolFactory, RequestFamily } from '../index';
import type { DescribeQuorumTopicInput } from './v0/request';
import { describeQuorumRequestV0 } from './v0/request';
import { describeQuorumResponseV0 } from './v0/response';

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

export interface DescribeQuorumOptions {
  topics?: DescribeQuorumTopicInput[];
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeQuorumOptions>>> = {
  0: (options) => ({
    request: describeQuorumRequestV0({ topics: options.topics ?? [] }),
    response: describeQuorumResponseV0,
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
