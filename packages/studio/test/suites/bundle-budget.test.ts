import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gzipSync } from 'node:zlib';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildProduction } from '../helpers/production-build';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');
const RUNNER_NODE_ENV = process.env.NODE_ENV;

/**
 * An initial load has to compete with the CLI's own near-instant startup, and a lazy route chunk
 * arrives on a click, not a page load — both stay small enough that neither ever reads as "the
 * studio is slow".
 *
 * The original target for initial JS was 140 KB gzip. After per-route code-splitting (every
 * `routes/*.lazy.tsx`) and deferring the sidebar's profile switcher (the one eager consumer of the
 * `Select` primitive), a production build still measures **~154 KB gzip** — about 14 KB over that
 * target. What's left is React 19 + TanStack Router/Query + the `Tooltip` primitive (mounted
 * unconditionally by `AppShell`, not optional — it backs the icon-only sidebar's accessible names)
 * + the zod contracts shared with the server. The budget below is set at the measured baseline
 * plus a couple of KB of slack, not at the original 140 KB: this is an honest ceiling on today's
 * bundle, catching further regressions, not a reopened attempt at the original number. Closing the
 * remaining gap needs a real further change — swapping the browser-side contracts to `zod/mini` is
 * the next lever — not a wider budget.
 *
 * Bumped from 156 KB to 168 KB for a round of dependency updates (`react`/`react-dom`/
 * `lucide-react`/`tailwind-merge`/`zod`/`@tanstack/react-router`/`@tanstack/react-virtual`/
 * `oxc-transform-react`), which moved the measured baseline to ~162.3 KB gzip.
 */
const INITIAL_JS_BUDGET_BYTES = 168 * 1024;
const INITIAL_CSS_BUDGET_BYTES = 20 * 1024;
const LAZY_ROUTE_CHUNK_BUDGET_BYTES = 60 * 1024;
const TARBALL_BUDGET_BYTES = 1.5 * 1024 * 1024;

interface ManifestChunk {
  readonly file: string;
  readonly src?: string;
  readonly isEntry?: boolean;
  readonly css?: readonly string[];
  readonly imports?: readonly string[];
}

type Manifest = Readonly<Record<string, ManifestChunk>>;

function gzipSizeOf(outDir: string, file: string): number {
  return gzipSync(readFileSync(join(outDir, file))).byteLength;
}

/** The transitive `imports` graph of the entry chunk — exactly what a fresh page load fetches before any route renders (this is the same graph Vite turns into `<link rel="modulepreload">` tags). */
function collectInitialChunkKeys(manifest: Manifest, entryKey: string): Set<string> {
  const visited = new Set<string>();
  function visit(key: string): void {
    if (visited.has(key)) return;
    visited.add(key);
    for (const imported of manifest[key]?.imports ?? []) visit(imported);
  }
  visit(entryKey);
  return visited;
}

describe('web bundle budget', () => {
  let outDir: string;
  let manifest: Manifest;

  beforeAll(() => {
    outDir = mkdtempSync(join(tmpdir(), 'kafka-studio-bundle-budget-'));
    buildProduction('vite.web.config.ts', outDir, { manifest: true });
    manifest = JSON.parse(readFileSync(join(outDir, '.vite/manifest.json'), 'utf8')) as Manifest;
  }, 60_000);

  afterAll(() => {
    if (outDir !== undefined) rmSync(outDir, { recursive: true, force: true });
  });

  it('measures production JSX without changing the test environment', () => {
    expect(process.env.NODE_ENV).toBe(RUNNER_NODE_ENV);
    const sources = Object.values(manifest)
      .filter((chunk) => chunk.file.endsWith('.js'))
      .flatMap((chunk) => {
        const sourceMap = JSON.parse(readFileSync(join(outDir, `${chunk.file}.map`), 'utf8')) as {
          sources: readonly string[];
        };
        return sourceMap.sources;
      });
    expect(sources.length).toBeGreaterThan(0);
    expect(sources.filter((source) => source.includes('jsx-dev-runtime'))).toEqual([]);
  });

  it('keeps the initial JS (shell + first route) under budget', () => {
    const entryKey = Object.keys(manifest).find((key) => manifest[key]?.isEntry === true);
    expect(entryKey).toBeDefined();

    const initialKeys = collectInitialChunkKeys(manifest, entryKey ?? '');
    const files = new Set<string>();
    for (const key of initialKeys) {
      const chunk = manifest[key];
      if (chunk === undefined) continue;
      files.add(chunk.file);
    }

    const totalGzipBytes = [...files].reduce((total, file) => total + gzipSizeOf(outDir, file), 0);
    expect(
      totalGzipBytes,
      `initial JS is ${(totalGzipBytes / 1024).toFixed(1)}KB gzipped (budget ${String(INITIAL_JS_BUDGET_BYTES / 1024)}KB): ${[...files].join(', ')}`,
    ).toBeLessThanOrEqual(INITIAL_JS_BUDGET_BYTES);
  });

  it('keeps the initial CSS under budget', () => {
    const entryKey = Object.keys(manifest).find((key) => manifest[key]?.isEntry === true);
    expect(entryKey).toBeDefined();

    const initialKeys = collectInitialChunkKeys(manifest, entryKey ?? '');
    const cssFiles = new Set<string>();
    for (const key of initialKeys) {
      for (const css of manifest[key]?.css ?? []) cssFiles.add(css);
    }

    const totalGzipBytes = [...cssFiles].reduce((total, file) => total + gzipSizeOf(outDir, file), 0);
    expect(totalGzipBytes).toBeLessThanOrEqual(INITIAL_CSS_BUDGET_BYTES);
  });

  it('keeps every lazy route chunk under budget', () => {
    // Each `routes/*.lazy.tsx` source is its own code-split route (see `src/web/router.tsx`) — one
    // chunk per route by construction, so this asserts on all of them, not a hand-picked sample.
    const routeChunks = Object.entries(manifest).filter(([key]) => key.endsWith('.lazy.tsx'));
    expect(routeChunks.length).toBeGreaterThan(0);

    for (const [key, chunk] of routeChunks) {
      const size = gzipSizeOf(outDir, chunk.file);
      expect(
        size,
        `${key} is ${(size / 1024).toFixed(1)}KB gzipped (budget ${String(LAZY_ROUTE_CHUNK_BUDGET_BYTES / 1024)}KB)`,
      ).toBeLessThanOrEqual(LAZY_ROUTE_CHUNK_BUDGET_BYTES);
    }
  });

  it('writes a brotli sibling for every compressible asset over the precompression threshold', () => {
    const compressible = Object.values(manifest).filter((chunk) => /\.(js|css)$/.test(chunk.file));
    let checked = 0;
    for (const chunk of compressible) {
      const identitySize = statSync(join(outDir, chunk.file)).size;
      if (identitySize < 1024) continue;
      checked += 1;
      expect(() => statSync(join(outDir, `${chunk.file}.br`))).not.toThrow();
    }
    expect(checked).toBeGreaterThan(0);
  });
});

describe('published tarball budget', () => {
  let tarballPath: string;
  let packSource: string;

  beforeAll(() => {
    // Self-contained build, mirroring tarball.test.ts's own isolated-build approach — this suite
    // cares about the resulting .tgz's byte size, not (again) its import safety.
    packSource = mkdtempSync(join(tmpdir(), 'kafka-studio-bundle-budget-pack-'));
    const distDir = join(packSource, 'dist');

    buildProduction('vite.config.ts', distDir);
    buildProduction('vite.web.config.ts', join(distDir, 'web'));
    execFileSync('pnpm', ['exec', 'tsc', '-p', 'tsconfig.json', '--emitDeclarationOnly', '--outDir', distDir], {
      cwd: PACKAGE_ROOT,
    });

    const manifest = JSON.parse(readFileSync(join(PACKAGE_ROOT, 'package.json'), 'utf8')) as Record<string, unknown>;
    writeFileSync(join(packSource, 'package.json'), JSON.stringify(manifest));
    writeFileSync(join(packSource, 'README.md'), readFileSync(join(PACKAGE_ROOT, 'README.md')));

    const packOutputDir = mkdtempSync(join(tmpdir(), 'kafka-studio-bundle-budget-tgz-'));
    const packOutput = execFileSync(
      'npm',
      ['pack', '--json', '--pack-destination', packOutputDir, '--ignore-scripts'],
      { cwd: packSource, encoding: 'utf8' },
    );
    const [packed] = JSON.parse(packOutput) as { filename: string }[];
    if (packed === undefined) throw new Error('npm pack produced no tarball');
    tarballPath = join(packOutputDir, packed.filename);
  }, 300_000);

  afterAll(() => {
    if (packSource !== undefined) rmSync(packSource, { recursive: true, force: true });
    if (tarballPath !== undefined) rmSync(dirname(tarballPath), { recursive: true, force: true });
  });

  it('never ships a sourcemap', () => {
    const listing = execFileSync('tar', ['-tzf', tarballPath], { encoding: 'utf8' });
    const mapEntries = listing.split('\n').filter((line) => line.endsWith('.map'));
    expect(mapEntries).toEqual([]);
  });

  it('keeps the packed tarball under budget', () => {
    const { size } = statSync(tarballPath);
    expect(
      size,
      `tarball is ${(size / 1024 / 1024).toFixed(2)}MB (budget ${String(TARBALL_BUDGET_BYTES / 1024 / 1024)}MB)`,
    ).toBeLessThanOrEqual(TARBALL_BUDGET_BYTES);
  });
});
