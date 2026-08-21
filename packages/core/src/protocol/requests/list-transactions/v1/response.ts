import type { ResponseDefinition } from '../../../schema';
import {
  listTransactionsResponseV0,
  type ListTransactionsResponseV0Body,
  type ListTransactionsState,
} from '../v0/response';

export type ListTransactionsResponseV1Body = ListTransactionsResponseV0Body;

export type { ListTransactionsState };

/** ListTransactions v1 has the same response wire format as v0 (KIP-994). */
export const listTransactionsResponseV1: ResponseDefinition<ListTransactionsResponseV1Body> =
  listTransactionsResponseV0;
