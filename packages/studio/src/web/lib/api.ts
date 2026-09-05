/**
 * The one place a studio API response turns into data or an `Error`. Every caller used to
 * hand-roll this, and three of them ignored the server's `{ error: { message } }` envelope, so
 * the same failure surfaced with a different sentence depending on which screen you were on.
 */

interface ErrorEnvelope {
  readonly error?: { readonly message?: string };
}

export async function assertOk(res: Response, what: string): Promise<void> {
  if (res.ok) return;
  const body = (await res.json().catch(() => null)) as ErrorEnvelope | null;
  throw new Error(body?.error?.message ?? `${what} failed with ${String(res.status)}`);
}

export async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path);
  await assertOk(res, `GET ${path}`);
  return (await res.json()) as T;
}

export async function apiSend<T>(method: 'POST' | 'PATCH' | 'DELETE', path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    ...(body === undefined ? {} : { headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }),
  });
  await assertOk(res, `${method} ${path}`);
  return (await res.json()) as T;
}

/**
 * `GET /api/health` also returns the studio's own `version`; it is deliberately not modelled here
 * because the UI never shows a version number.
 */
export interface HealthResponse {
  readonly status: string;
  readonly readOnly: boolean;
  readonly uptimeSeconds: number;
}

export interface ClusterStatus {
  readonly connected: boolean;
}

export const healthQueryKey = ['health'] as const;
export const clusterQueryKey = ['cluster'] as const;

export const fetchHealth = (): Promise<HealthResponse> => apiGet<HealthResponse>('/api/health');
export const fetchClusterStatus = (): Promise<ClusterStatus> => apiGet<ClusterStatus>('/api/cluster');
