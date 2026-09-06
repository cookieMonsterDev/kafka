import type { AclListResponse, QuotaListResponse, TransactionListResponse } from '../../shared/contracts/settings';
import { apiGet } from './api';

export const settingsQueryKeys = {
  acls: ['acls'] as const,
  quotas: ['quotas'] as const,
  transactions: ['transactions'] as const,
};

export const listAcls = (): Promise<AclListResponse> => apiGet<AclListResponse>('/api/acls');
export const listQuotas = (): Promise<QuotaListResponse> => apiGet<QuotaListResponse>('/api/quotas');
export const listTransactions = (): Promise<TransactionListResponse> =>
  apiGet<TransactionListResponse>('/api/transactions');
