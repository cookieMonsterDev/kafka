import type { ProtocolFactory, RequestFamily } from '../index';
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

const VERSIONS: Readonly<Record<number, ProtocolFactory<ShareGroupDescribeOptions>>> = {
  1: (options) => ({
    request: shareGroupDescribeRequestV1(fields(options)),
    response: shareGroupDescribeResponseV1,
  }),
};

export const ShareGroupDescribe: RequestFamily<ShareGroupDescribeOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ShareGroupDescribe protocol for version ${version}`);
    return factory;
  },
});
