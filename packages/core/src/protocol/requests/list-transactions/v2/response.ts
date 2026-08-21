import type { ResponseDefinition } from '../../../schema';
import {
  listTransactionsResponseV0,
  type ListTransactionsResponseV0Body,
  type ListTransactionsState,
} from '../v0/response';

export type ListTransactionsResponseV2Body = ListTransactionsResponseV0Body;

export type { ListTransactionsState };

/** ListTransactions v2 has the same response wire format as v0. */
export const listTransactionsResponseV2: ResponseDefinition<ListTransactionsResponseV2Body> =
  listTransactionsResponseV0;
