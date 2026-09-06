import type { DeleteRecordsRequest, MessagesPageResponse } from '../../shared/contracts/message';
import { apiGet, apiSend } from './api';

export interface MessagesPageQuery {
  readonly partition?: number;
  /** `"earliest"`, `"latest"`, or a decimal offset string — see `messagesQuerySchema`. */
  readonly from?: string;
  readonly timestamp?: number;
  readonly limit?: number;
}

export const messagesQueryKeys = {
  all: ['messages'] as const,
  page: (topic: string, query: MessagesPageQuery) => [...messagesQueryKeys.all, 'page', topic, query] as const,
};

function toSearchParams(query: MessagesPageQuery): string {
  const params = new URLSearchParams();
  if (query.partition !== undefined) params.set('partition', String(query.partition));
  if (query.from !== undefined) params.set('from', query.from);
  if (query.timestamp !== undefined) params.set('timestamp', String(query.timestamp));
  if (query.limit !== undefined) params.set('limit', String(query.limit));
  const search = params.toString();
  return search === '' ? '' : `?${search}`;
}

export function listMessages(topic: string, query: MessagesPageQuery = {}): Promise<MessagesPageResponse> {
  return apiGet(`/api/topics/${encodeURIComponent(topic)}/messages${toSearchParams(query)}`);
}

export function tailUrl(topic: string, partition?: number): string {
  const search = partition === undefined ? '' : `?partition=${String(partition)}`;
  return `/api/topics/${encodeURIComponent(topic)}/tail${search}`;
}

export function deleteRecords(topic: string, input: DeleteRecordsRequest): Promise<{ topic: string }> {
  return apiSend('POST', `/api/topics/${encodeURIComponent(topic)}/records/delete`, input);
}
