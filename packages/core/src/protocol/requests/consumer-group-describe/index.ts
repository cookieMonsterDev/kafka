import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
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

const VERSIONS = {
  0: (options) => ({
    request: consumerGroupDescribeRequestV0(fields(options)),
    response: consumerGroupDescribeResponseV0,
  }),
  1: (options) => ({
    request: consumerGroupDescribeRequestV1(fields(options)),
    response: consumerGroupDescribeResponseV1,
  }),
} satisfies VersionFactories<ConsumerGroupDescribeOptions>;

export const ConsumerGroupDescribe = defineRequestFamily(KAFKA_APIS.ConsumerGroupDescribe, VERSIONS);
