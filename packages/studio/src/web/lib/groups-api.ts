import type {
  DeleteGroupOffsetsRequest,
  DeleteGroupOffsetsResponse,
  GroupDetailResponse,
  GroupListResponse,
  RemoveGroupMembersRequest,
  RemoveGroupMembersResponse,
  ResetGroupOffsetsRequest,
  ResetGroupOffsetsResponse,
  ShareGroupDetailResponse,
  ShareGroupListResponse,
} from '../../shared/contracts/group';

export const groupQueryKeys = {
  all: ['groups'] as const,
  list: () => [...groupQueryKeys.all, 'list'] as const,
  detail: (groupId: string) => [...groupQueryKeys.all, 'detail', groupId] as const,
};

export const shareGroupQueryKeys = {
  all: ['share-groups'] as const,
  list: () => [...shareGroupQueryKeys.all, 'list'] as const,
  detail: (groupId: string) => [...shareGroupQueryKeys.all, 'detail', groupId] as const,
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

export async function listGroups(): Promise<GroupListResponse> {
  const res = await fetch('/api/groups');
  return parseJsonOrThrow(res, 'GET /api/groups');
}

export async function getGroup(groupId: string): Promise<GroupDetailResponse> {
  const res = await fetch(`/api/groups/${encodeURIComponent(groupId)}`);
  return parseJsonOrThrow(res, 'GET /api/groups/:id');
}

export async function resetGroupOffsets(
  groupId: string,
  input: ResetGroupOffsetsRequest,
): Promise<ResetGroupOffsetsResponse> {
  const res = await fetch(`/api/groups/${encodeURIComponent(groupId)}/offsets/reset`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'POST /api/groups/:id/offsets/reset');
}

export async function deleteGroupOffsets(
  groupId: string,
  input: DeleteGroupOffsetsRequest,
): Promise<DeleteGroupOffsetsResponse> {
  const res = await fetch(`/api/groups/${encodeURIComponent(groupId)}/offsets`, {
    method: 'DELETE',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'DELETE /api/groups/:id/offsets');
}

export async function removeGroupMembers(
  groupId: string,
  input: RemoveGroupMembersRequest,
): Promise<RemoveGroupMembersResponse> {
  const res = await fetch(`/api/groups/${encodeURIComponent(groupId)}/members/remove`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(input),
  });
  return parseJsonOrThrow(res, 'POST /api/groups/:id/members/remove');
}

export async function deleteGroup(groupId: string): Promise<void> {
  const res = await fetch(`/api/groups/${encodeURIComponent(groupId)}`, { method: 'DELETE' });
  await assertOk(res, 'DELETE /api/groups/:id');
}

export async function listShareGroups(): Promise<ShareGroupListResponse> {
  const res = await fetch('/api/share-groups');
  return parseJsonOrThrow(res, 'GET /api/share-groups');
}

export async function getShareGroup(groupId: string): Promise<ShareGroupDetailResponse> {
  const res = await fetch(`/api/share-groups/${encodeURIComponent(groupId)}`);
  return parseJsonOrThrow(res, 'GET /api/share-groups/:id');
}
