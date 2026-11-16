import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeProducersRequestV0, type DescribeProducersRequestV0Options } from './v0/request';
import { describeProducersResponseV0 } from './v0/response';

export type { DescribeProducersRequestV0Options, DescribeProducersRequestV0Topic } from './v0/request';
export type {
  DescribeProducersResponseV0ActiveProducer,
  DescribeProducersResponseV0Body,
  DescribeProducersResponseV0Partition,
  DescribeProducersResponseV0Topic,
} from './v0/response';

const VERSIONS = {
  0: (values) => ({
    request: describeProducersRequestV0(values),
    response: describeProducersResponseV0,
  }),
} satisfies VersionFactories<DescribeProducersRequestV0Options>;

export const DescribeProducers = defineRequestFamily(KAFKA_APIS.DescribeProducers, VERSIONS);
