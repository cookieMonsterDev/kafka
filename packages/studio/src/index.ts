import { fileURLToPath } from 'node:url';
import { createStudioServer } from './server/create-server';
import { createDevMiddleware } from './server/dev';
import { AdminPool } from './server/kafka/admin-pool';
import { createKafkaClient, resolveStudioConnectionConfig } from './server/kafka/connection';
import { openBrowser, formatBanner } from './server/open-browser';
import { resolvePort } from './server/port';
import { registerClusterRoutes } from './server/routes/cluster';
import { registerHealthRoutes } from './server/routes/health';
import { registerProfileRoutes } from './server/routes/profiles';
import { registerTopicRoutes } from './server/routes/topics';
import { Router } from './server/router';
import { createStaticHandler } from './server/static';
import { readStudioSection } from './server/studio-config';
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
 * Starts the studio server: resolves the config file, a port, and the route table, wires up
 * static (or, in dev mode, Vite middleware) serving, listens, opens a browser, and prints the
 * startup banner. The returned handle's `stop()` is the one real shutdown path — there is no
 * implicit cleanup on process exit, so a caller embedding this as a library is responsible for
 * calling it.
 */
export async function startStudio(options: StudioOptions, runtime: Runtime): Promise<StudioHandle> {
  const connection = await resolveStudioConnectionConfig({ cwd: runtime.cwd, env: runtime.env });
  const studioConfig = readStudioSection(connection.fileConfig, (message) =>
    runtime.stderr.write(`kafka-studio: ${message}\n`),
  );

  const host = options.host ?? studioConfig.host ?? DEFAULT_HOST;
  const readOnly = options.readOnly ?? studioConfig.readOnly ?? false;
  const browser = options.browser ?? (studioConfig.openBrowser === false ? 'none' : undefined);
  const port = await resolvePort({ host, requestedPort: options.port ?? studioConfig.port });
  const startedAt = runtime.now();
  const version = readOwnVersion(import.meta.url);

  const pool = new AdminPool((profileName) => createKafkaClient(connection, profileName));
  let activeProfile: string | null = null;

  const router = new Router();
  registerHealthRoutes(router, { version, readOnly, host, port, startedAt });
  registerClusterRoutes(router);
  registerProfileRoutes(router, {
    connection,
    pool,
    getActiveProfile: () => activeProfile,
    setActiveProfile: (profile) => {
      activeProfile = profile;
    },
  });
  registerTopicRoutes(router, { pool, getActiveProfile: () => activeProfile });

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
  void openBrowser(url, browser, { env: runtime.env, platform: runtime.platform });

  async function stop(): Promise<void> {
    await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
    await pool.disposeAll();
  }

  return { url, host, port, stop };
}
