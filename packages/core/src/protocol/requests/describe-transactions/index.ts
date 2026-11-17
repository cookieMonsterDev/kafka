import { defineRequestFamily, type VersionFactories } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { describeTransactionsRequestV0, type DescribeTransactionsRequestV0Fields } from './v0/request';
import { describeTransactionsResponseV0 } from './v0/response';

export type DescribeTransactionsOptions = DescribeTransactionsRequestV0Fields;

const VERSIONS = {
  0: (options) => ({
    request: describeTransactionsRequestV0(options),
    response: describeTransactionsResponseV0,
  }),
} satisfies VersionFactories<DescribeTransactionsOptions>;

export const DescribeTransactions = defineRequestFamily(KAFKA_APIS.DescribeTransactions, VERSIONS);
