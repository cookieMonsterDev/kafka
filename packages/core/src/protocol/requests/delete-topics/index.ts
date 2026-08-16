import type { ProtocolFactory, RequestFamily } from '../index';
import { deleteTopicsRequestV0 } from './v0/request';
import { deleteTopicsResponseV0 } from './v0/response';
import { deleteTopicsRequestV1 } from './v1/request';
import { deleteTopicsResponseV1 } from './v1/response';
import { deleteTopicsRequestV2 } from './v2/request';
import { deleteTopicsResponseV2 } from './v2/response';
import { deleteTopicsRequestV3 } from './v3/request';
import { deleteTopicsResponseV3 } from './v3/response';
import { deleteTopicsRequestV4 } from './v4/request';
import { deleteTopicsResponseV4 } from './v4/response';
import { deleteTopicsRequestV5 } from './v5/request';
import { deleteTopicsResponseV5 } from './v5/response';
import { deleteTopicsRequestV6 } from './v6/request';
import { deleteTopicsResponseV6 } from './v6/response';

export interface DeleteTopicsOptions {
  topics: string[];
  timeout?: number;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DeleteTopicsOptions>>> = {
  0: (options) => ({
    request: deleteTopicsRequestV0({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV0,
  }),
  1: (options) => ({
    request: deleteTopicsRequestV1({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV1,
  }),
  2: (options) => ({
    request: deleteTopicsRequestV2({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV2,
  }),
  3: (options) => ({
    request: deleteTopicsRequestV3({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV3,
  }),
  4: (options) => ({
    request: deleteTopicsRequestV4({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV4,
  }),
  5: (options) => ({
    request: deleteTopicsRequestV5({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV5,
  }),
  6: (options) => ({
    request: deleteTopicsRequestV6({ topics: options.topics, timeout: options.timeout ?? 5000 }),
    response: deleteTopicsResponseV6,
  }),
};

export const DeleteTopics: RequestFamily<DeleteTopicsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteTopics protocol for version ${version}`);
    return factory;
  },
});
