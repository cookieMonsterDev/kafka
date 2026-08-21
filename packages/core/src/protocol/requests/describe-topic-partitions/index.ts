import type { ProtocolFactory, RequestFamily } from '../index';
import { describeTopicPartitionsRequestV0, type DescribeTopicPartitionsRequestV0Options } from './v0/request';
import { describeTopicPartitionsResponseV0 } from './v0/response';

export type {
  DescribeTopicPartitionsRequestV0Cursor,
  DescribeTopicPartitionsRequestV0Options,
  DescribeTopicPartitionsRequestV0Topic,
} from './v0/request';
export { DEFAULT_RESPONSE_PARTITION_LIMIT } from './v0/request';
export type {
  DescribeTopicPartitionsResponseV0Body,
  DescribeTopicPartitionsResponseV0Cursor,
  DescribeTopicPartitionsResponseV0Partition,
  DescribeTopicPartitionsResponseV0Topic,
} from './v0/response';

export type DescribeTopicPartitionsOptions = DescribeTopicPartitionsRequestV0Options;

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeTopicPartitionsOptions>>> = {
  0: (values) => ({
    request: describeTopicPartitionsRequestV0(values),
    response: describeTopicPartitionsResponseV0,
  }),
};

export const DescribeTopicPartitions: RequestFamily<DescribeTopicPartitionsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeTopicPartitions protocol for version ${version}`);
    return factory;
  },
});
