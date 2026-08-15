import type { ProtocolFactory, RequestFamily } from '../index.js';
import { type DescribeConfigsResource, describeConfigsRequestV1, withDefaultConfigNames } from './v1/request.js';
import { describeConfigsResponseV1 } from './v1/response.js';
import { describeConfigsRequestV2 } from './v2/request.js';
import { describeConfigsResponseV2 } from './v2/response.js';

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
 * Kafka 4.0.0's real floor for this API is v1, not v0: `DescribeConfigsRequest.json`'s
 * `validVersions` is `"1-4"` (advertised live as min=1, max=4), and no librdkafka-style
 * advertising override applies here (only Produce has one) — kafkajs's v0 is genuinely gone.
 */
export const DescribeConfigs: RequestFamily<DescribeConfigsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeConfigs protocol for version ${version}`);
    return factory;
  },
});
