import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { shareGroupDescribeRequestV1 } from './v1/request';
import { shareGroupDescribeResponseV1 } from './v1/response';

export type { ShareGroupDescribeResponseV1Body } from './v1/response';

export interface ShareGroupDescribeOptions {
  groupIds: string[];
  includeAuthorizedOperations?: boolean;
}

const fields = (options: ShareGroupDescribeOptions) => ({
  groupIds: options.groupIds,
  includeAuthorizedOperations: options.includeAuthorizedOperations ?? false,
});

const VERSIONS = {
  1: (options) => ({
    request: shareGroupDescribeRequestV1(fields(options)),
    response: shareGroupDescribeResponseV1,
  }),
} satisfies VersionFactories<ShareGroupDescribeOptions>;

export const ShareGroupDescribe = defineRequestFamily(KAFKA_APIS.ShareGroupDescribe, VERSIONS);
