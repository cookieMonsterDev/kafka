import type { ProtocolFactory, RequestFamily } from '../index';
import { type DescribeConfigsResource, describeConfigsRequestV1, withDefaultConfigNames } from './v1/request';
import { describeConfigsResponseV1 } from './v1/response';
import { describeConfigsRequestV2 } from './v2/request';
import { describeConfigsResponseV2 } from './v2/response';

export interface DescribeConfigsOptions {
  resources: DescribeConfigsResource[];
  includeSynonyms?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeConfigsOptions>>> = {
  1: (options) => ({
    request: describeConfigsRequestV1({
      resources: withDefaultConfigNames(options.resources),
      includeSynonyms: options.includeSynonyms ?? false,
    }),
    response: describeConfigsResponseV1,
  }),
  2: (options) => ({
    request: describeConfigsRequestV2({
      resources: withDefaultConfigNames(options.resources),
      includeSynonyms: options.includeSynonyms ?? false,
    }),
    response: describeConfigsResponseV2,
  }),
};

/**
 * Kafka 4.0+ advertises DescribeConfigs as versions 1-4; v0 is not implemented.
 *
 * @see https://kafka.apache.org/43/configuration/topic-configs/
 */
export const DescribeConfigs: RequestFamily<DescribeConfigsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeConfigs protocol for version ${version}`);
    return factory;
  },
});
