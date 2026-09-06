import { sendJson } from '../create-server';
import type { Router } from '../router';

export interface HealthContext {
  readonly version: string;
  readonly readOnly: boolean;
  readonly host: string;
  readonly port: number;
  readonly startedAt: Date;
}

/**
 * `GET /api/health` for scripted/monitoring use, and `GET /__studio_runtime.json` for the SPA
 * itself — the same server state, injected into the browser at request time (not baked into the
 * built bundle) so a rebuild isn't needed to pick up a different port or mode. `no-store` because
 * this must never be served from a cache.
 */
export function registerHealthRoutes(router: Router, context: HealthContext): void {
  router.get('/api/health', (_req, res) => {
    sendJson(res, 200, {
      status: 'ok',
      version: context.version,
      readOnly: context.readOnly,
      uptimeSeconds: Math.floor((Date.now() - context.startedAt.getTime()) / 1000),
    });
  });

  router.get('/__studio_runtime.json', (_req, res) => {
    res.setHeader('cache-control', 'no-store');
    sendJson(res, 200, {
      version: context.version,
      readOnly: context.readOnly,
      host: context.host,
      port: context.port,
    });
  });
}
