import type {
  AlterTopicConfigsRequest,
  CreatePartitionsRequest,
  CreateTopicRequest,
  TopicDetailResponse,
  TopicListResponse,
} from '../../shared/contracts/topic';
import { withAuthHeaders } from './auth';

export const topicQueryKeys = {
  all: ['topics'] as const,
  list: () => [...topicQueryKeys.all, 'list'] as const,
  detail: (name: string) => [...topicQueryKeys.all, 'detail', name] as const,
};

async function assertOk(res: Response, what: string): Promise<void> {
  if (res.ok) return;
  const body = (await res.json().catch(() => null)) as { error?: { message?: string } } | null;
  throw new Error(body?.error?.message ?? `${what} failed with ${String(res.status)}`);
}

async function parseJsonOrThrow<T>(res: Response, what: string): Promise<T> {
  await assertOk(res, what);
  return (await res.json()) as T;
}

export async function listTopics(): Promise<TopicListResponse> {
  const res = await fetch('/api/topics', { headers: withAuthHeaders() });
  return parseJsonOrThrow(res, 'GET /api/topics');
}

export async function getTopic(name: string): Promise<TopicDetailResponse> {
  const res = await fetch(`/api/topics/${encodeURIComponent(name)}`, { headers: withAuthHeaders() });
  return parseJsonOrThrow(res, 'GET /api/topics/:name');
}

export async function createTopic(input: CreateTopicRequest): Promise<{ topic: string }> {
  const res = await fetch('/api/topics', {
    method: 'POST',
    headers: withAuthHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'POST /api/topics');
}

export async function deleteTopic(name: string): Promise<void> {
  const res = await fetch(`/api/topics/${encodeURIComponent(name)}`, { method: 'DELETE', headers: withAuthHeaders() });
  await assertOk(res, 'DELETE /api/topics/:name');
}

export async function addPartitions(
  name: string,
  input: CreatePartitionsRequest,
): Promise<{ topic: string; count: number }> {
  const res = await fetch(`/api/topics/${encodeURIComponent(name)}/partitions`, {
    method: 'POST',
    headers: withAuthHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'POST /api/topics/:name/partitions');
}

export async function alterTopicConfigs(name: string, input: AlterTopicConfigsRequest): Promise<{ topic: string }> {
  const res = await fetch(`/api/topics/${encodeURIComponent(name)}/configs`, {
    method: 'PATCH',
    headers: withAuthHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'PATCH /api/topics/:name/configs');
}
