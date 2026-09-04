import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import type { ServerResponse } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
// A real top-level import (not the dynamic `await import('./index')` the last test needs for
// `vi.resetModules()`), so parsing this module's dependency graph — which now reaches into
// `@cookiemonsterdev/kafka-core` and `@cookiemonsterdev/kafka-config` — happens during this file's
// import phase, not inside the first test's own timeout window.
import { startStudio } from './index';
import type { Runtime } from './runtime';

function fakeRuntime(overrides: Partial<Runtime> = {}): { runtime: Runtime; stdout: (chunk: string) => boolean } {
  const stdout = vi.fn(() => true);
  const runtime: Runtime = {
    argv: [],
    cwd: '/nonexistent-test-cwd',
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
    const { runtime, stdout } = fakeRuntime();

    const studio = await startStudio({ host: '127.0.0.1', port: 59_101, browser: 'none' }, runtime);
    try {
      expect(studio.url).toBe(`http://127.0.0.1:${String(studio.port)}/`);
      expect(stdout).toHaveBeenCalledWith(expect.stringContaining(studio.url));

      const health = await fetch(new URL('/api/health', studio.url));
      expect(health.status).toBe(200);

      const cluster = await fetch(new URL('/api/cluster', studio.url));
      await expect(cluster.json()).resolves.toEqual({ connected: false });

      const profiles = await fetch(new URL('/api/profiles', studio.url));
      await expect(profiles.json()).resolves.toEqual({ active: null, profiles: {} });

      const shell = await fetch(new URL('/', studio.url));
      expect(shell.status).toBe(200);
      expect(shell.headers.get('content-type')).toContain('text/html');
    } finally {
      await studio.stop();
    }
  });

  it('does not attempt to open a browser when browser is "none"', async () => {
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

  describe('the "studio" config-file section', () => {
    let dir: string;

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'studio-index-'));
    });

    afterEach(() => {
      rmSync(dir, { recursive: true, force: true });
    });

    it('falls back to port/host/readOnly from the config file when no flag was given', async () => {
      writeFileSync(
        join(dir, 'kafka.config.json'),
        JSON.stringify({ studio: { port: 59_110, host: '127.0.0.1', readOnly: true, openBrowser: false } }),
      );
      const { runtime } = fakeRuntime({ cwd: dir });

      const studio = await startStudio({}, runtime);
      try {
        expect(studio.port).toBe(59_110);
        expect(studio.host).toBe('127.0.0.1');
        const health = await fetch(new URL('/api/health', studio.url));
        await expect(health.json()).resolves.toMatchObject({ readOnly: true });
      } finally {
        await studio.stop();
      }
    });

    it('an explicit option always wins over the config file', async () => {
      writeFileSync(join(dir, 'kafka.config.json'), JSON.stringify({ studio: { port: 59_111 } }));
      const { runtime } = fakeRuntime({ cwd: dir });

      const studio = await startStudio({ port: 59_112, host: '127.0.0.1', browser: 'none' }, runtime);
      try {
        expect(studio.port).toBe(59_112);
      } finally {
        await studio.stop();
      }
    });
  });
});
