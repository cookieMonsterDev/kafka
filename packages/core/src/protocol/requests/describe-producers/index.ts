import type { ProtocolFactory, RequestFamily } from '../index';
import { describeProducersRequestV0, type DescribeProducersRequestV0Options } from './v0/request';
import { describeProducersResponseV0 } from './v0/response';

export type { DescribeProducersRequestV0Options, DescribeProducersRequestV0Topic } from './v0/request';
export type {
  DescribeProducersResponseV0ActiveProducer,
  DescribeProducersResponseV0Body,
  DescribeProducersResponseV0Partition,
  DescribeProducersResponseV0Topic,
} from './v0/response';

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeProducersRequestV0Options>>> = {
  0: (values) => ({
    request: describeProducersRequestV0(values),
    response: describeProducersResponseV0,
  }),
};

export const DescribeProducers: RequestFamily<DescribeProducersRequestV0Options> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeProducers protocol for version ${version}`);
    return factory;
  },
});
