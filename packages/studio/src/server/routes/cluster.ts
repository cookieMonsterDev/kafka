import { sendJson } from '../create-server';
import type { Router } from '../router';

export interface ClusterStatus {
  readonly connected: false;
}

/**
 * `GET /api/cluster` — reports connection status only for now. Resolving a real broker connection
 * needs a config file and a profile, neither of which exist yet; the shape here is deliberately
 * small so the UI can render a "not connected" state without lying about data it doesn't have.
 */
export function registerClusterRoutes(router: Router): void {
  router.get('/api/cluster', (_req, res) => {
    const status: ClusterStatus = { connected: false };
    sendJson(res, 200, status);
  });
}
