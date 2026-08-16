import type { ProtocolFactory, RequestFamily } from '../index';
import { electLeadersRequestV0, type ElectLeadersTopicPartitions } from './v0/request';
import { electLeadersResponseV0 } from './v0/response';
import { electLeadersRequestV1 } from './v1/request';
import { electLeadersResponseV1 } from './v1/response';
import { electLeadersRequestV2 } from './v2/request';
import { electLeadersResponseV2 } from './v2/response';

export interface ElectLeadersOptions {
  topicPartitions?: ElectLeadersTopicPartitions[] | null;
  electionType?: number;
  timeout?: number;
}

const DEFAULT_TIMEOUT = 5000;
const PREFERRED_ELECTION = 0;

const VERSIONS: Readonly<Record<number, ProtocolFactory<ElectLeadersOptions>>> = {
  0: (options) => ({
    request: electLeadersRequestV0({
      timeout: options.timeout ?? DEFAULT_TIMEOUT,
      topicPartitions: options.topicPartitions ?? [],
    }),
    response: electLeadersResponseV0,
  }),
  1: (options) => ({
    request: electLeadersRequestV1({
      electionType: options.electionType ?? PREFERRED_ELECTION,
      timeout: options.timeout ?? DEFAULT_TIMEOUT,
      topicPartitions: options.topicPartitions ?? [],
    }),
    response: electLeadersResponseV1,
  }),
  2: (options) => ({
    request: electLeadersRequestV2({
      electionType: options.electionType ?? PREFERRED_ELECTION,
      timeout: options.timeout ?? DEFAULT_TIMEOUT,
      topicPartitions: options.topicPartitions === undefined ? null : options.topicPartitions,
    }),
    response: electLeadersResponseV2,
  }),
};

export const ElectLeaders: RequestFamily<ElectLeadersOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ElectLeaders protocol for version ${version}`);
    return factory;
  },
});
