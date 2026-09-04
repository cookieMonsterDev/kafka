import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import path from 'node:path';

const CONTENT_TYPES: Readonly<Record<string, string>> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
  '.map': 'application/json; charset=utf-8',
  '.woff2': 'font/woff2',
};

function contentTypeFor(filePath: string): string {
  return CONTENT_TYPES[path.extname(filePath)] ?? 'application/octet-stream';
}

function etagFor(size: number, mtimeMs: number): string {
  return `"${createHash('sha1')
    .update(`${String(size)}-${String(mtimeMs)}`)
    .digest('hex')}"`;
}

/** Has a file extension, i.e. looks like a real asset request rather than a client-side route. */
function looksLikeAssetPath(pathname: string): boolean {
  return path.extname(pathname) !== '';
}

async function serveFile(filePath: string, res: ServerResponse, req: IncomingMessage): Promise<void> {
  const stats = await stat(filePath);
  const etag = etagFor(stats.size, stats.mtimeMs);

  if (req.headers['if-none-match'] === etag) {
    res.writeHead(304, { etag });
    res.end();
    return;
  }

  res.writeHead(200, {
    'content-type': contentTypeFor(filePath),
    'content-length': stats.size,
    etag,
    // Filenames are content-hashed by the web build; a long-lived cache is safe for assets, but
    // index.html itself must always be revalidated so a new deploy is picked up immediately.
    'cache-control': path.basename(filePath) === 'index.html' ? 'no-cache' : 'public, max-age=31536000, immutable',
  });
  createReadStream(filePath).pipe(res);
}

/**
 * Serves the built SPA from `webRoot`. Returns a handler compatible with
 * {@link import('./create-server').CreateServerOptions.fallback}: `true` once it has written a
 * response, `false` to let the caller produce its own 404 (a missing asset with a real extension,
 * e.g. a stale chunk URL — SPA-falling-back to `index.html` there would hide the real problem).
 */
export function createStaticHandler(
  webRoot: string,
): (req: IncomingMessage, res: ServerResponse, url: URL) => Promise<boolean> {
  return async (req, res, url) => {
    const requested = path.normalize(path.join(webRoot, decodeURIComponent(url.pathname)));
    // Reject path traversal outside webRoot (e.g. `/../../etc/passwd`) before touching the filesystem.
    if (!requested.startsWith(path.normalize(webRoot + path.sep)) && requested !== path.normalize(webRoot)) {
      return false;
    }

    try {
      const stats = await stat(requested);
      if (stats.isFile()) {
        await serveFile(requested, res, req);
        return true;
      }
    } catch {
      // Not a file (or doesn't exist) — fall through to the SPA-fallback logic below.
    }

    if (looksLikeAssetPath(url.pathname)) return false;

    try {
      await serveFile(path.join(webRoot, 'index.html'), res, req);
      return true;
    } catch {
      return false;
    }
  };
}
