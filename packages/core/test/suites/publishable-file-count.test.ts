import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');

/**
 * Headroom above the 818 files this suite packs (dist, README, package.json). It catches a
 * regression such as re-enabled maps, test files back in the declaration build or a return to one
 * JavaScript file per source module, not ordinary growth. Lower it when a build change shrinks the
 * file count.
 */
const FILE_COUNT_CEILING = 900;

/** The bundled build emits only these JavaScript files; a per-module build would emit ~800. */
const JS_FILES = [
  'dist/index.js',
  'dist/protocol/compression/codec-worker.js',
  'dist/protocol/compression/off-thread.js',
];

/** `new Worker(...)` loads this file by path at runtime, so it must survive any build change. */
const CODEC_WORKER_PATH = 'dist/protocol/compression/codec-worker.js';

interface PackedFile {
  path: string;
}

interface PackResult {
  files: PackedFile[];
}

describe('published file count', () => {
  let packSource: string;
  let packed: PackResult;

  beforeAll(async () => {
    // Self-contained, into an isolated temp dir — never the package's own `dist/` — so this
    // suite's build cannot race a sibling suite that is still importing from it. `npm pack` runs
    // here too, not inside the `it` below: under a loaded machine (e.g. the pre-commit hook's
    // `pnpm -r test` running every package's suite at once) spawning `npm` can outrun vitest's
    // default 5s per-test timeout on its own, and this whole setup already has a generous 60s.
    packSource = mkdtempSync(join(tmpdir(), 'kafka-core-pack-source-'));
    const distDir = join(packSource, 'dist');

    const { build } = await import('vite');
    await build({
      configFile: join(PACKAGE_ROOT, 'vite.config.ts'),
      logLevel: 'silent',
      build: { outDir: distDir, emptyOutDir: true },
    });
    execFileSync('pnpm', ['exec', 'tsc', '-p', 'tsconfig.build.json', '--emitDeclarationOnly', '--outDir', distDir], {
      cwd: PACKAGE_ROOT,
    });
    cpSync(join(PACKAGE_ROOT, 'package.json'), join(packSource, 'package.json'));
    cpSync(join(PACKAGE_ROOT, 'README.md'), join(packSource, 'README.md'));

    const output = execFileSync('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], {
      cwd: packSource,
      encoding: 'utf8',
    });
    const [result] = JSON.parse(output) as PackResult[];
    if (result === undefined) throw new Error('npm pack --dry-run produced no result');
    packed = result;
  }, 60_000);

  function builtUrl(file: string): string {
    return pathToFileURL(join(packSource, 'dist', file)).href;
  }

  /**
   * Runs `script` as an ES module in a plain Node process inside the packed build, so the built
   * files load the way a consumer loads them, without Vitest's transforms. The script and a
   * `node_modules` link for the bare imports are added after packing, so neither reaches the tarball.
   */
  function runInBuild(script: string): string {
    const scriptPath = join(packSource, 'check.mjs');
    const modulesLink = join(packSource, 'node_modules');
    writeFileSync(scriptPath, script);
    symlinkSync(join(PACKAGE_ROOT, 'node_modules'), modulesLink, 'dir');
    try {
      return execFileSync(process.execPath, [scriptPath], { cwd: packSource, encoding: 'utf8', timeout: 30_000 });
    } finally {
      rmSync(modulesLink, { force: true });
      rmSync(scriptPath, { force: true });
    }
  }

  afterAll(() => {
    if (packSource !== undefined) rmSync(packSource, { recursive: true, force: true });
  });

  it('ships no source or declaration maps', () => {
    const maps = packed.files.filter((file) => file.path.endsWith('.map'));
    expect(maps).toEqual([]);
  });

  it('ships no declarations for test files', () => {
    const testDeclarations = packed.files.filter(
      (file) => file.path.endsWith('.test.d.ts') || file.path.includes('test-defaults'),
    );
    expect(testDeclarations).toEqual([]);
  });

  it('ships the codec worker entry', () => {
    expect(packed.files.map((file) => file.path)).toContain(CODEC_WORKER_PATH);
  });

  it('stays under the file-count ceiling', () => {
    expect(packed.files.length).toBeLessThan(FILE_COUNT_CEILING);
  });

  it('ships the bundled entry, codec pool and codec worker as the only JavaScript files', () => {
    const jsFiles = packed.files.map((file) => file.path).filter((path) => path.endsWith('.js'));
    expect(jsFiles.sort()).toEqual(JS_FILES);
  });

  it('exports from the built entry what src/index.ts exports', async () => {
    const sourceExports = Object.keys(await import('../../src/index')).sort();
    const output = runInBuild(`
      const core = await import(${JSON.stringify(builtUrl('index.js'))});
      console.log(JSON.stringify(Object.keys(core).sort()));
    `);
    expect(JSON.parse(output)).toEqual(sourceExports);
  }, 30_000);

  // `runCodecOp` quietly falls back to the main thread when the worker cannot start, so a round
  // trip alone would not show a broken worker path; count the threads Node actually started.
  it('round-trips Snappy and LZ4 through codec worker threads from the built entry', () => {
    const output = runInBuild(`
      let workers = 0;
      process.on('worker', () => workers++);
      const core = await import(${JSON.stringify(builtUrl('index.js'))});
      const roundTrips = {};
      for (const type of ['Snappy', 'LZ4']) {
        const codec = core.CompressionCodecs[core.CompressionTypes[type]]();
        const input = Buffer.from(type.repeat(4096));
        const compressed = await codec.compress({ buffer: input });
        roundTrips[type] = (await codec.decompress(compressed)).equals(input);
      }
      const { stopCodecWorkers } = await import(${JSON.stringify(builtUrl('protocol/compression/off-thread.js'))});
      await stopCodecWorkers();
      console.log(JSON.stringify({ roundTrips, startedWorkers: workers > 0 }));
    `);
    expect(JSON.parse(output)).toEqual({ roundTrips: { Snappy: true, LZ4: true }, startedWorkers: true });
  }, 30_000);
});
