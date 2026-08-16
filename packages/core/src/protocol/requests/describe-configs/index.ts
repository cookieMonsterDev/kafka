import type { ProtocolFactory, RequestFamily } from '../index';
import { describeConfigsRequestV0 } from './v0/request';
import { describeConfigsResponseV0 } from './v0/response';
import { type DescribeConfigsResource, describeConfigsRequestV1, withDefaultConfigNames } from './v1/request';
import { describeConfigsResponseV1 } from './v1/response';
import { describeConfigsRequestV2 } from './v2/request';
import { describeConfigsResponseV2 } from './v2/response';

export interface DescribeConfigsOptions {
  resources: DescribeConfigsResource[];
  /**
   * Request config synonyms. Ignored on DescribeConfigs v0 (Kafka 0.11);
   * the broker does not advertise the field and decoded entries have an empty
   * `configSynonyms` array.
   */
  includeSynonyms?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeConfigsOptions>>> = {
  0: (options) => ({
    request: describeConfigsRequestV0({
      resources: withDefaultConfigNames(options.resources),
    }),
    response: describeConfigsResponseV0,
  }),
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
 * Kafka 0.11 advertises DescribeConfigs v0 (no synonyms). Kafka 1.1+ adds v1
 * synonyms; Kafka 4.0+ typically advertises 1-4.
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
