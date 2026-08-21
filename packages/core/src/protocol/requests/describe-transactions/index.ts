import type { ProtocolFactory, RequestFamily } from '../index';
import { describeTransactionsRequestV0, type DescribeTransactionsRequestV0Fields } from './v0/request';
import { describeTransactionsResponseV0 } from './v0/response';

export type DescribeTransactionsOptions = DescribeTransactionsRequestV0Fields;

const VERSIONS: Readonly<Record<number, ProtocolFactory<DescribeTransactionsOptions>>> = {
  0: (options) => ({
    request: describeTransactionsRequestV0(options),
    response: describeTransactionsResponseV0,
  }),
};

export const DescribeTransactions: RequestFamily<DescribeTransactionsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no DescribeTransactions protocol for version ${version}`);
    return factory;
  },
});
