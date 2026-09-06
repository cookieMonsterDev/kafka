import { sendJson } from '../create-server';
import type { AdminPool } from '../kafka/admin-pool';
import type { Router } from '../router';

export type ClusterStatus =
  | { readonly connected: true; readonly brokerCount: number; readonly clusterId: string | null }
  | { readonly connected: false };

export interface ClusterRouteContext {
  readonly pool: AdminPool;
  getActiveProfile(): string | null;
}

/**
 * `GET /api/cluster` — actually probes the connection this time: reuses (or opens) the pooled
 * admin for the active profile and calls `describeCluster`. Any failure along the way — the
 * broker's unreachable, auth failed, whatever — reports `connected: false` rather than throwing;
 * this endpoint's whole job is to turn "can we reach it" into a status the UI can render either way.
 */
export function registerClusterRoutes(router: Router, context: ClusterRouteContext): void {
  router.get('/api/cluster', async (_req, res) => {
    try {
      const admin = await context.pool.get(context.getActiveProfile());
      const description = await admin.describeCluster();
      const status: ClusterStatus = {
        connected: true,
        brokerCount: description.brokers.length,
        clusterId: description.clusterId,
      };
      sendJson(res, 200, status);
    } catch {
      const status: ClusterStatus = { connected: false };
      sendJson(res, 200, status);
    }
  });
}
