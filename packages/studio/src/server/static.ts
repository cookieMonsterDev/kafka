import { createHash } from 'node:crypto';
import { createReadStream, type Stats } from 'node:fs';
import { stat } from 'node:fs/promises';
import type { IncomingMessage, ServerResponse } from 'node:http';
import path from 'node:path';

/**
 * Checked in priority order against a precompressed `<file>.br`/`<file>.gz` sibling the web build
 * writes alongside each compressible asset (see `vite.web.config.ts`). Brotli first — it
 * compresses smaller than gzip for the same content, so it wins whenever the client advertises
 * both (as every modern browser does).
 */
const PRECOMPRESSED_ENCODINGS: readonly { readonly encoding: string; readonly suffix: string }[] = [
  { encoding: 'br', suffix: '.br' },
  { encoding: 'gzip', suffix: '.gz' },
];

/**
 * A minimal `Accept-Encoding` check: true unless the header explicitly disables `encoding` with
 * `;q=0`. Good enough for negotiating against exactly two candidates, in priority order — this is
 * not a general content-negotiation implementation.
 */
function acceptsEncoding(header: string | undefined, encoding: string): boolean {
  if (header === undefined) return false;
  return header.split(',').some((part) => {
    const [token, ...params] = part.trim().split(';');
    if (token !== encoding && token !== '*') return false;
    const qParam = params.find((param) => param.trim().startsWith('q='));
    if (qParam === undefined) return true;
    return Number(qParam.trim().slice(2)) > 0;
  });
}

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

/**
 * Whether a miss should 404 instead of falling back to the SPA.
 *
 * Only extensions the web build actually emits count. Treating *any* dot as an extension breaks
 * client-side routes that carry a dot in a path segment — and Kafka topic names conventionally do
 * (`/topics/orders.created`), so that heuristic made every such topic un-deep-linkable.
 */
function looksLikeAssetPath(pathname: string): boolean {
  const extension = path.extname(pathname).toLowerCase();
  return extension !== '' && extension in CONTENT_TYPES;
}

async function statPrecompressedVariant(
  filePath: string,
  acceptEncodingHeader: string | undefined,
): Promise<{ readonly path: string; readonly encoding: string; readonly stats: Stats } | null> {
  for (const candidate of PRECOMPRESSED_ENCODINGS) {
    if (!acceptsEncoding(acceptEncodingHeader, candidate.encoding)) continue;
    try {
      const variantPath = filePath + candidate.suffix;
      const stats = await stat(variantPath);
      return { path: variantPath, encoding: candidate.encoding, stats };
    } catch {
      // No precompressed sibling for this encoding — try the next, then fall back to identity.
    }
  }
  return null;
}

async function serveFile(filePath: string, res: ServerResponse, req: IncomingMessage): Promise<void> {
  const identityStats = await stat(filePath);
  const variant = await statPrecompressedVariant(filePath, req.headers['accept-encoding']);
  const stats = variant?.stats ?? identityStats;
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
    // Present whenever a precompressed variant *could* have been chosen, not only when it was —
    // a shared cache must know this response varies on the request header even on the identity path.
    vary: 'Accept-Encoding',
    ...(variant !== null ? { 'content-encoding': variant.encoding } : {}),
    // Filenames are content-hashed by the web build; a long-lived cache is safe for assets, but
    // index.html itself must always be revalidated so a new deploy is picked up immediately.
    'cache-control': path.basename(filePath) === 'index.html' ? 'no-cache' : 'public, max-age=31536000, immutable',
  });
  createReadStream(variant?.path ?? filePath).pipe(res);
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
