import type {
  ApiVersionsResponse,
  BrokerApiVersionsView,
  LogDirListResponse,
  LogDirView,
} from '../../shared/contracts/cluster';
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

  /**
   * `GET /api/cluster/log-dirs` — every broker's log dirs with their volume sizes. An empty topic
   * list asks the brokers for no partitions, so the response stays small on a large cluster.
   */
  router.get('/api/cluster/log-dirs', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { brokers } = await admin.describeLogDirs({ topics: [] });

    const logDirs: LogDirView[] = brokers.flatMap(({ brokerId, logDirs: brokerLogDirs }) =>
      brokerLogDirs.map((logDir) => {
        const { totalBytes, usableBytes } = logDir;
        const hasSizes = totalBytes !== undefined && usableBytes !== undefined;
        return {
          brokerId,
          path: logDir.logDir,
          totalBytes: hasSizes ? String(totalBytes) : null,
          usableBytes: hasSizes ? String(usableBytes) : null,
          usedBytes: hasSizes ? String(totalBytes - usableBytes) : null,
          isCordoned: logDir.isCordoned ?? null,
        };
      }),
    );
    logDirs.sort((a, b) => a.brokerId - b.brokerId || a.path.localeCompare(b.path));

    const response: LogDirListResponse = { logDirs };
    sendJson(res, 200, response);
  });

  /**
   * `GET /api/cluster/api-versions` — every broker's advertised version range for each Kafka API,
   * with the version Studio's client negotiates and the APIs it does not implement (KIP-1220).
   */
  router.get('/api/cluster/api-versions', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { brokers } = await admin.describeApiVersions();

    const views: BrokerApiVersionsView[] = brokers.map(({ nodeId, host, port, rack, apis }) => ({
      nodeId,
      host,
      port,
      rack,
      apis: apis.map((api) => ({
        apiKey: api.apiKey,
        apiName: api.apiName ?? null,
        brokerVersions: api.brokerVersions ?? null,
        isImplemented: api.isImplemented,
        version: api.version ?? null,
      })),
    }));
    views.sort((a, b) => a.nodeId - b.nodeId);

    const response: ApiVersionsResponse = { brokers: views };
    sendJson(res, 200, response);
  });
}
