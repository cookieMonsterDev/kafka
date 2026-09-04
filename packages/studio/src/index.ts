import { fileURLToPath } from 'node:url';
import { createStudioServer } from './server/create-server';
import { createDevMiddleware } from './server/dev';
import { openBrowser, formatBanner } from './server/open-browser';
import { resolvePort } from './server/port';
import { registerClusterRoutes } from './server/routes/cluster';
import { registerHealthRoutes } from './server/routes/health';
import { Router } from './server/router';
import { createStaticHandler } from './server/static';
import type { Runtime } from './runtime';
import { readOwnVersion } from './version';

export interface StudioOptions {
  readonly port?: number;
  readonly host?: string;
  /** `undefined` defers to `BROWSER`/the platform default; `'none'` disables opening entirely. */
  readonly browser?: string;
  readonly readOnly?: boolean;
}

export interface StudioHandle {
  readonly url: string;
  readonly host: string;
  readonly port: number;
  stop(): Promise<void>;
}

const DEFAULT_HOST = '127.0.0.1';

/**
 * Starts the studio server: resolves a port, builds the route table, wires up static (or, in dev
 * mode, Vite middleware) serving, listens, opens a browser, and prints the startup banner. The
 * returned handle's `stop()` is the one real shutdown path — there is no implicit cleanup on
 * process exit, so a caller embedding this as a library is responsible for calling it.
 */
export async function startStudio(options: StudioOptions, runtime: Runtime): Promise<StudioHandle> {
  const host = options.host ?? DEFAULT_HOST;
  const readOnly = options.readOnly ?? false;
  const port = await resolvePort({ host, requestedPort: options.port });
  const startedAt = runtime.now();
  const version = readOwnVersion(import.meta.url);

  const router = new Router();
  registerHealthRoutes(router, { version, readOnly, host, port, startedAt });
  registerClusterRoutes(router);

  const webRoot = fileURLToPath(new URL('./web/', import.meta.url));
  const fallback =
    runtime.env.KAFKA_STUDIO_DEV === '1' ? await createDevMiddleware(webRoot) : createStaticHandler(webRoot);

  const server = createStudioServer({ router, fallback });

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve());
  });

  const url = `http://${host}:${String(port)}/`;
  runtime.stdout.write(`${formatBanner({ url, readOnly })}\n`);
  void openBrowser(url, options.browser, { env: runtime.env, platform: runtime.platform });

  return {
    url,
    host,
    port,
    stop: () => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))),
  };
}
