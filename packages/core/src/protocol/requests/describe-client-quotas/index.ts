import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { type DescribeClientQuotasComponent, describeClientQuotasRequestV0 } from './v0/request';
import { describeClientQuotasResponseV0 } from './v0/response';
import { describeClientQuotasRequestV1 } from './v1/request';
import { describeClientQuotasResponseV1 } from './v1/response';

export type { DescribeClientQuotasComponent };

export interface DescribeClientQuotasOptions {
  components: DescribeClientQuotasComponent[];
  strict?: boolean;
}

const VERSIONS = {
  0: (options) => ({
    request: describeClientQuotasRequestV0({
      components: options.components,
      strict: options.strict ?? false,
    }),
    response: describeClientQuotasResponseV0,
  }),
  1: (options) => ({
    request: describeClientQuotasRequestV1({
      components: options.components,
      strict: options.strict ?? false,
    }),
    response: describeClientQuotasResponseV1,
  }),
} satisfies VersionFactories<DescribeClientQuotasOptions>;

export const DescribeClientQuotas = defineRequestFamily(KAFKA_APIS.DescribeClientQuotas, VERSIONS);
