import type { ServerResponse } from 'node:http';
import { describe, expect, it, vi } from 'vitest';
import type { Runtime } from './runtime';

function fakeRuntime(overrides: Partial<Runtime> = {}): { runtime: Runtime; stdout: (chunk: string) => boolean } {
  const stdout = vi.fn(() => true);
  const runtime: Runtime = {
    argv: [],
    env: {},
    platform: 'linux',
    stdout: { write: stdout },
    stderr: { write: vi.fn(() => true) },
    now: () => new Date(),
    exit: () => {
      throw new Error('exit() should not be called by startStudio');
    },
    signal: new AbortController().signal,
    ...overrides,
  };
  return { runtime, stdout };
}

describe('startStudio', () => {
  it('binds to a free port, serves the built shell, and exposes health/cluster routes', async () => {
    const { startStudio } = await import('./index');
    const { runtime, stdout } = fakeRuntime();

    const studio = await startStudio({ host: '127.0.0.1', port: 59_101, browser: 'none' }, runtime);
    try {
      expect(studio.url).toBe(`http://127.0.0.1:${String(studio.port)}/`);
      expect(stdout).toHaveBeenCalledWith(expect.stringContaining(studio.url));

      const health = await fetch(new URL('/api/health', studio.url));
      expect(health.status).toBe(200);

      const cluster = await fetch(new URL('/api/cluster', studio.url));
      await expect(cluster.json()).resolves.toEqual({ connected: false });

      const shell = await fetch(new URL('/', studio.url));
      expect(shell.status).toBe(200);
      expect(shell.headers.get('content-type')).toContain('text/html');
    } finally {
      await studio.stop();
    }
  });

  it('does not attempt to open a browser when browser is "none"', async () => {
    const { startStudio } = await import('./index');
    const { runtime } = fakeRuntime();

    const studio = await startStudio({ port: 59_102, browser: 'none' }, runtime);
    try {
      // No direct spy on the module-private spawn call; absence of a thrown/unhandled rejection
      // plus the explicit "none" contract (covered in server/open-browser.test.ts) is the check.
      expect(studio.port).toBeGreaterThan(0);
    } finally {
      await studio.stop();
    }
  });

  it('requires an explicit port to be free, rather than silently picking another one', async () => {
    const { startStudio } = await import('./index');
    const first = await startStudio({ host: '127.0.0.1', port: 59_103, browser: 'none' }, fakeRuntime().runtime);
    try {
      await expect(
        startStudio({ host: '127.0.0.1', port: first.port, browser: 'none' }, fakeRuntime().runtime),
      ).rejects.toThrow(`port ${String(first.port)} is already in use`);
    } finally {
      await first.stop();
    }
  });

  it('routes through vite middleware instead of static files when KAFKA_STUDIO_DEV=1', async () => {
    const middlewares = vi.fn((_req, res: ServerResponse, next: (error?: unknown) => void) => {
      res.writeHead(200, { 'content-type': 'text/html' });
      res.end('<html>dev</html>');
      next();
    });
    vi.doMock('vite', () => ({ createServer: vi.fn(async () => ({ middlewares })) }));
    vi.resetModules();

    const { startStudio } = await import('./index');
    const { runtime } = fakeRuntime({ env: { KAFKA_STUDIO_DEV: '1' } });
    const studio = await startStudio({ port: 59_104, browser: 'none' }, runtime);
    try {
      const res = await fetch(new URL('/', studio.url));
      expect(middlewares).toHaveBeenCalled();
      await expect(res.text()).resolves.toBe('<html>dev</html>');
    } finally {
      await studio.stop();
      vi.doUnmock('vite');
      vi.resetModules();
    }
  });
});
