import type { IncomingMessage, ServerResponse } from 'node:http';

/**
 * Dev-mode fallback: proxies unmatched requests through Vite's own middleware, so `src/web` is
 * served with HMR instead of a `dist/web` build. `vite` is imported lazily — it's a
 * `devDependency`, and the published package must load fine without it (see {@link createStaticHandler}
 * for the production equivalent).
 */
export async function createDevMiddleware(
  webRoot: string,
): Promise<(req: IncomingMessage, res: ServerResponse, url: URL) => Promise<boolean>> {
  const { createServer } = await import('vite');
  const vite = await createServer({
    root: webRoot,
    server: { middlewareMode: true },
    appType: 'spa',
  });

  return (req, res) =>
    new Promise((resolve, reject) => {
      vite.middlewares(req, res, (error?: unknown) => {
        if (error) reject(error instanceof Error ? error : new Error('vite middleware failed'));
        // Vite's own middleware always ends the response itself (including its SPA fallback to
        // index.html), so reaching the `next()` callback with no error still counts as handled.
        else resolve(true);
      });
    });
}
