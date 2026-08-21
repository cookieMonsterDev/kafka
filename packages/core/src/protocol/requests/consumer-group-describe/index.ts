import type { ProtocolFactory, RequestFamily } from '../index';
import { consumerGroupDescribeRequestV0 } from './v0/request';
import { consumerGroupDescribeResponseV0 } from './v0/response';
import { consumerGroupDescribeRequestV1 } from './v1/request';
import { consumerGroupDescribeResponseV1 } from './v1/response';

export type { ConsumerGroupDescribeResponseV0Body } from './v0/response';
export type { ConsumerGroupDescribeResponseV1Body } from './v1/response';

export interface ConsumerGroupDescribeOptions {
  groupIds: string[];
  includeAuthorizedOperations?: boolean;
}

const fields = (options: ConsumerGroupDescribeOptions) => ({
  groupIds: options.groupIds,
  includeAuthorizedOperations: options.includeAuthorizedOperations ?? false,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<ConsumerGroupDescribeOptions>>> = {
  0: (options) => ({
    request: consumerGroupDescribeRequestV0(fields(options)),
    response: consumerGroupDescribeResponseV0,
  }),
  1: (options) => ({
    request: consumerGroupDescribeRequestV1(fields(options)),
    response: consumerGroupDescribeResponseV1,
  }),
};

export const ConsumerGroupDescribe: RequestFamily<ConsumerGroupDescribeOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ConsumerGroupDescribe protocol for version ${version}`);
    return factory;
  },
});
