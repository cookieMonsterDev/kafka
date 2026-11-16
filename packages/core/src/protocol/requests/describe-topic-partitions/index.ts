import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeTopicPartitionsRequestV0, type DescribeTopicPartitionsRequestV0Options } from './v0/request';
import { describeTopicPartitionsResponseV0 } from './v0/response';

export type {
  DescribeTopicPartitionsRequestV0Cursor,
  DescribeTopicPartitionsRequestV0Options,
  DescribeTopicPartitionsRequestV0Topic,
} from './v0/request';
export type {
  DescribeTopicPartitionsResponseV0Body,
  DescribeTopicPartitionsResponseV0Cursor,
  DescribeTopicPartitionsResponseV0Partition,
  DescribeTopicPartitionsResponseV0Topic,
} from './v0/response';

export type DescribeTopicPartitionsOptions = DescribeTopicPartitionsRequestV0Options;

const VERSIONS = {
  0: (values) => ({
    request: describeTopicPartitionsRequestV0(values),
    response: describeTopicPartitionsResponseV0,
  }),
} satisfies VersionFactories<DescribeTopicPartitionsOptions>;

export const DescribeTopicPartitions = defineRequestFamily(KAFKA_APIS.DescribeTopicPartitions, VERSIONS);
