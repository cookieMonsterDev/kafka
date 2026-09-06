import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { createStudioServer } from './create-server';
import { Router } from './router';
import { createStaticHandler } from './static';

async function withServer<T>(webRoot: string, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const server = createStudioServer({ router: new Router(), fallback: createStaticHandler(webRoot) });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('createStaticHandler', () => {
  let webRoot: string;

  beforeEach(async () => {
    webRoot = await mkdtemp(path.join(tmpdir(), 'kafka-studio-static-'));
    await writeFile(path.join(webRoot, 'index.html'), '<html><body>index</body></html>');
    await mkdir(path.join(webRoot, 'assets'));
    await writeFile(path.join(webRoot, 'assets', 'app.js'), 'console.log(1)');
  });

  afterEach(async () => {
    await rm(webRoot, { recursive: true, force: true });
  });

  it('serves a real asset with its content type', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/assets/app.js`);
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toContain('text/javascript');
      await expect(res.text()).resolves.toBe('console.log(1)');
    });
  });

  it('falls back to index.html for a client-side route', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/topics/orders`);
      expect(res.status).toBe(200);
      await expect(res.text()).resolves.toContain('index');
    });
  });

  it('serves index.html for the root path', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/`);
      expect(res.status).toBe(200);
      await expect(res.text()).resolves.toContain('index');
    });
  });

  it('falls back to index.html for a route segment containing a dot', async () => {
    await withServer(webRoot, async (baseUrl) => {
      // Kafka topic names conventionally use dots, so `.created` must not read as a file extension.
      const res = await fetch(`${baseUrl}/topics/orders.created`);
      expect(res.status).toBe(200);
      await expect(res.text()).resolves.toContain('index');
    });
  });

  it('returns 404 for a missing asset with a known extension, without falling back', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/assets/missing.js`);
      expect(res.status).toBe(404);
    });
  });

  it('serves index.html with a no-cache directive and assets as immutable', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const index = await fetch(`${baseUrl}/`);
      expect(index.headers.get('cache-control')).toBe('no-cache');
      const asset = await fetch(`${baseUrl}/assets/app.js`);
      expect(asset.headers.get('cache-control')).toContain('immutable');
    });
  });

  it('responds 304 when If-None-Match matches the current ETag', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const first = await fetch(`${baseUrl}/assets/app.js`);
      const etag = first.headers.get('etag');
      expect(etag).toBeTruthy();

      const second = await fetch(`${baseUrl}/assets/app.js`, { headers: { 'if-none-match': etag ?? '' } });
      expect(second.status).toBe(304);
    });
  });

  it('rejects path traversal outside webRoot', async () => {
    await withServer(webRoot, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/..%2f..%2fetc%2fpasswd`);
      expect(res.status).toBe(404);
    });
  });

  describe('precompressed asset negotiation', () => {
    beforeEach(async () => {
      // Deliberately different bytes per encoding, not just a re-gzip of the same content — a test
      // that served identity content while claiming `content-encoding: br` would still pass a
      // byte-for-byte comparison against the original, undici transparently decompresses the body.
      await writeFile(path.join(webRoot, 'assets', 'app.js.br'), 'console.log("br")');
      await writeFile(path.join(webRoot, 'assets', 'app.js.gz'), 'console.log("gzip")');
    });

    it('prefers a brotli sibling when the request accepts it', async () => {
      await withServer(webRoot, async (baseUrl) => {
        const res = await fetch(`${baseUrl}/assets/app.js`, { headers: { 'accept-encoding': 'br, gzip' } });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-encoding')).toBe('br');
        expect(res.headers.get('vary')).toBe('Accept-Encoding');
      });
    });

    it('falls back to gzip when the request does not accept brotli', async () => {
      await withServer(webRoot, async (baseUrl) => {
        const res = await fetch(`${baseUrl}/assets/app.js`, { headers: { 'accept-encoding': 'gzip' } });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-encoding')).toBe('gzip');
      });
    });

    it('falls back to identity when the request accepts neither', async () => {
      await withServer(webRoot, async (baseUrl) => {
        const res = await fetch(`${baseUrl}/assets/app.js`, { headers: { 'accept-encoding': 'identity' } });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-encoding')).toBeNull();
        expect(res.headers.get('vary')).toBe('Accept-Encoding');
        await expect(res.text()).resolves.toBe('console.log(1)');
      });
    });

    it('honours an explicit q=0 disabling an otherwise-accepted encoding', async () => {
      await withServer(webRoot, async (baseUrl) => {
        const res = await fetch(`${baseUrl}/assets/app.js`, { headers: { 'accept-encoding': 'br;q=0, gzip' } });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-encoding')).toBe('gzip');
      });
    });

    it('falls back to identity for an asset with no precompressed sibling', async () => {
      await withServer(webRoot, async (baseUrl) => {
        await writeFile(path.join(webRoot, 'assets', 'plain.txt'), 'plain');
        const res = await fetch(`${baseUrl}/assets/plain.txt`, { headers: { 'accept-encoding': 'br, gzip' } });
        expect(res.status).toBe(200);
        expect(res.headers.get('content-encoding')).toBeNull();
      });
    });
  });
});
