import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');

/**
 * Measured against this suite's own build, immediately before `declarationMap: false` was added
 * to `tsconfig.json`: 4,683 files, 1,559 of them `.d.ts.map`. `files: ["dist"]` in package.json
 * never publishes `src`, so a `.d.ts.map`'s `sources` pointer to `../src/...` resolves to nothing
 * in an installed tarball — dead weight on the `npx` cold path, with no upside. This ceiling is
 * generous headroom above the ~3,124 files that remain, not a tight bound: it exists to catch a
 * regression (an accidental revert of `declarationMap: false`), not to police ordinary growth.
 */
const FILE_COUNT_CEILING = 3500;

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
    execFileSync('pnpm', ['exec', 'tsc', '-p', 'tsconfig.json', '--emitDeclarationOnly', '--outDir', distDir], {
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

  afterAll(() => {
    if (packSource !== undefined) rmSync(packSource, { recursive: true, force: true });
  });

  it('ships zero .d.ts.map files and stays under the file-count ceiling', () => {
    const declarationMaps = packed.files.filter((file) => file.path.endsWith('.d.ts.map'));
    expect(declarationMaps).toEqual([]);
    expect(packed.files.length).toBeLessThan(FILE_COUNT_CEILING);
  });
});
