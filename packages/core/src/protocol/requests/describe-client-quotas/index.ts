import type { ProtocolFactory, RequestFamily } from '../index';
import { type DescribeClientQuotasComponent, describeClientQuotasRequestV0 } from './v0/request';
import { describeClientQuotasResponseV0 } from './v0/response';
import { describeClientQuotasRequestV1 } from './v1/request';
import { describeClientQuotasResponseV1 } from './v1/response';

export type { DescribeClientQuotasComponent };

export interface DescribeClientQuotasOptions {
  components: DescribeClientQuotasComponent[];
  strict?: boolean;
}

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeClientQuotasOptions>>> = {
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
};

export const DescribeClientQuotas: RequestFamily<DescribeClientQuotasOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeClientQuotas protocol for version ${version}`);
    return factory;
  },
});
