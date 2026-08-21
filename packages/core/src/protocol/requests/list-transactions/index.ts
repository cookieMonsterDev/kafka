import type { ProtocolFactory, RequestFamily } from '../index';
import { listTransactionsRequestV0 } from './v0/request';
import { listTransactionsResponseV0 } from './v0/response';
import { listTransactionsRequestV1 } from './v1/request';
import { listTransactionsResponseV1 } from './v1/response';
import { listTransactionsRequestV2 } from './v2/request';
import { listTransactionsResponseV2 } from './v2/response';

export interface ListTransactionsOptions {
  stateFilters?: string[];
  producerIdFilters?: bigint[];
  durationFilter?: bigint;
  transactionalIdPattern?: string | null;
}

const v0Fields = (options: ListTransactionsOptions) => ({
  stateFilters: options.stateFilters ?? [],
  producerIdFilters: options.producerIdFilters ?? [],
});

const v1Fields = (options: ListTransactionsOptions) => ({
  ...v0Fields(options),
  durationFilter: options.durationFilter ?? -1n,
});

const VERSIONS: Readonly<Record<number, ProtocolFactory<ListTransactionsOptions>>> = {
  0: (options) => ({
    request: listTransactionsRequestV0(v0Fields(options)),
    response: listTransactionsResponseV0,
  }),
  1: (options) => ({
    request: listTransactionsRequestV1(v1Fields(options)),
    response: listTransactionsResponseV1,
  }),
  2: (options) => ({
    request: listTransactionsRequestV2({
      ...v1Fields(options),
      transactionalIdPattern: options.transactionalIdPattern ?? null,
    }),
    response: listTransactionsResponseV2,
  }),
};

export const ListTransactions: RequestFamily<ListTransactionsOptions> = Object.freeze({
  versions: Object.freeze(Object.keys(VERSIONS).map(Number)),
  protocol({ version }: { version: number }) {
    const factory = VERSIONS[version];
    if (!factory) throw new Error(`Invariant violated: no ListTransactions protocol for version ${version}`);
    return factory;
  },
});
