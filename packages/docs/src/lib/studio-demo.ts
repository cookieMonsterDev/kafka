import { cpSync, existsSync, readFileSync, statSync } from 'node:fs';
import { dirname, extname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { AstroIntegration } from 'astro';

/** Where the demo is published, relative to the site base. The Studio introduction frames `index.html` here. */
export const STUDIO_DEMO_PATH = 'studio-demo/';

const CONTENT_TYPES: Readonly<Record<string, string>> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.woff2': 'font/woff2',
};

/** The demo build Studio writes beside its own `dist/` (`packages/studio/vite.demo.config.ts`). */
function demoDirectory(): string {
  const entry = fileURLToPath(import.meta.resolve('@cookiemonsterdev/kafka-studio'));
  const directory = join(dirname(entry), 'demo');
  if (!existsSync(join(directory, 'index.html'))) {
    throw new Error(
      `Studio demo build not found at ${directory}. Build it with: pnpm --filter @cookiemonsterdev/kafka-docs... build`,
    );
  }
  return directory;
}

/**
 * Publishes Studio's demo build at `/studio-demo/`. The dev server reads `dist/demo` on every
 * request, so rebuilding the demo needs only a page reload, not a dev server restart; a build
 * copies the directory into the output.
 */
export function studioDemo(): AstroIntegration {
  let base = '/';
  return {
    name: 'studio-demo',
    hooks: {
      'astro:config:done': ({ config }) => {
        base = config.base.endsWith('/') ? config.base : `${config.base}/`;
      },
      'astro:server:setup': ({ server }) => {
        const directory = demoDirectory();
        server.middlewares.use(`${base}${STUDIO_DEMO_PATH}`, (req, res, next) => {
          const pathname = decodeURIComponent(new URL(req.url ?? '/', 'http://localhost').pathname);
          const file = resolve(directory, `.${pathname === '/' ? '/index.html' : pathname}`);
          const contentType = CONTENT_TYPES[extname(file)];
          const isInside = !relative(directory, file).startsWith(`..${sep}`);
          if (contentType === undefined || !isInside || !existsSync(file) || !statSync(file).isFile()) {
            next();
            return;
          }
          res.setHeader('Content-Type', contentType);
          res.setHeader('Cache-Control', 'no-store');
          res.end(readFileSync(file));
        });
      },
      'astro:build:done': ({ dir }) => {
        cpSync(demoDirectory(), fileURLToPath(new URL(STUDIO_DEMO_PATH, dir)), { recursive: true });
      },
    },
  };
}
