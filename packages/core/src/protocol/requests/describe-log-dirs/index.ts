import type { ProtocolFactory, RequestFamily } from '../index';
import { type DescribeLogDirsTopic, describeLogDirsRequestV0 } from './v0/request';
import { describeLogDirsResponseV0 } from './v0/response';
import { describeLogDirsRequestV1 } from './v1/request';
import { describeLogDirsResponseV1 } from './v1/response';
import { describeLogDirsRequestV2 } from './v2/request';
import { describeLogDirsResponseV2 } from './v2/response';

export interface DescribeLogDirsOptions {
  topics?: DescribeLogDirsTopic[] | null;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeLogDirsOptions>>> = {
  0: (options) => ({
    request: describeLogDirsRequestV0({ topics: options.topics ?? [] }),
    response: describeLogDirsResponseV0,
  }),
  1: (options) => ({
    request: describeLogDirsRequestV1({ topics: options.topics ?? [] }),
    response: describeLogDirsResponseV1,
  }),
  2: (options) => ({
    request: describeLogDirsRequestV2({ topics: options.topics === undefined ? null : options.topics }),
    response: describeLogDirsResponseV2,
  }),
};

export const DescribeLogDirs: RequestFamily<DescribeLogDirsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeLogDirs protocol for version ${version}`);
    return factory;
  },
});
