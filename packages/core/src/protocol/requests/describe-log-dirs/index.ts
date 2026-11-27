import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { type DescribeLogDirsTopic, describeLogDirsRequestV0 } from './v0/request';
import { describeLogDirsResponseV0 } from './v0/response';
import { describeLogDirsRequestV1 } from './v1/request';
import { describeLogDirsResponseV1 } from './v1/response';
import { describeLogDirsRequestV2 } from './v2/request';
import { describeLogDirsResponseV2 } from './v2/response';
import { describeLogDirsRequestV3 } from './v3/request';
import { describeLogDirsResponseV3 } from './v3/response';
import { describeLogDirsRequestV4 } from './v4/request';
import { describeLogDirsResponseV4 } from './v4/response';
import { describeLogDirsRequestV5 } from './v5/request';
import { describeLogDirsResponseV5 } from './v5/response';

export interface DescribeLogDirsOptions {
  topics?: DescribeLogDirsTopic[] | null;
}

const VERSIONS = {
  0: (options) => ({
    request: describeLogDirsRequestV0({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV0,
  }),
  1: (options) => ({
    request: describeLogDirsRequestV1({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV1,
  }),
  2: (options) => ({
    request: describeLogDirsRequestV2({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV2,
  }),
  3: (options) => ({
    request: describeLogDirsRequestV3({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV3,
  }),
  4: (options) => ({
    request: describeLogDirsRequestV4({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV4,
  }),
  5: (options) => ({
    request: describeLogDirsRequestV5({ topics: options.topics ?? null }),
    response: describeLogDirsResponseV5,
  }),
} satisfies VersionFactories<DescribeLogDirsOptions>;

export const DescribeLogDirs = defineRequestFamily(KAFKA_APIS.DescribeLogDirs, VERSIONS);
