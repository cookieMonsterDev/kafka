import type { ProtocolFactory, RequestFamily } from '../index';
import { deleteTopicsRequestV0 } from './v0/request';
import { deleteTopicsResponseV0 } from './v0/response';
import { deleteTopicsRequestV1 } from './v1/request';
import { deleteTopicsResponseV1 } from './v1/response';

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
};

export const DeleteTopics: RequestFamily<DeleteTopicsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DeleteTopics protocol for version ${version}`);
    return factory;
  },
});
