import type { ProtocolFactory, RequestFamily } from '../index';
import { describeConfigsRequestV0 } from './v0/request';
import { describeConfigsResponseV0 } from './v0/response';
import { type DescribeConfigsResource, describeConfigsRequestV1, withDefaultConfigNames } from './v1/request';
import { describeConfigsResponseV1 } from './v1/response';
import { describeConfigsRequestV2 } from './v2/request';
import { describeConfigsResponseV2 } from './v2/response';
import { describeConfigsRequestV3 } from './v3/request';
import { describeConfigsResponseV3 } from './v3/response';
import { describeConfigsRequestV4 } from './v4/request';
import { describeConfigsResponseV4 } from './v4/response';

export interface DescribeConfigsOptions {
  resources: DescribeConfigsResource[];
  /**
   * Request config synonyms. Ignored on DescribeConfigs v0 (Kafka 0.11);
   * the broker does not advertise the field and decoded entries have an empty
   * `configSynonyms` array.
   */
  includeSynonyms?: boolean;
  /**
   * Request per-entry documentation (KIP-524). Ignored on DescribeConfigs v0–v2;
   * decoded entries only include `documentation` / `configType` on v3+.
   */
  includeDocumentation?: boolean;
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
  3: (options) => ({
    request: describeConfigsRequestV3({
      resources: withDefaultConfigNames(options.resources),
      includeSynonyms: options.includeSynonyms ?? false,
      includeDocumentation: options.includeDocumentation ?? false,
    }),
    response: describeConfigsResponseV3,
  }),
  4: (options) => ({
    request: describeConfigsRequestV4({
      resources: withDefaultConfigNames(options.resources),
      includeSynonyms: options.includeSynonyms ?? false,
      includeDocumentation: options.includeDocumentation ?? false,
    }),
    response: describeConfigsResponseV4,
  }),
};

/**
 * Kafka 0.11 advertises DescribeConfigs v0 (no synonyms). Kafka 1.1+ adds v1
 * synonyms; Kafka 2.5+ v3 documentation (KIP-524); v4 is flexible (KIP-482).
 * Kafka 4.0+ typically advertises 1-4.
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
