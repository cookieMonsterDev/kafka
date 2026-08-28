import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');
const INSTALLED_PACKAGE_DIR = 'node_modules/@cookiemonsterdev/kafka-config';

const RELATIVE_OR_NODE_IMPORT_PATTERN = /^(?:\.\.?\/|node:)/;
// Anchored to line start/dynamic-import syntax specifically (not a bare "from"/"import" anywhere
// in the file) so a string like "...if (x) return {" inside an error message never matches.
const STATIC_IMPORT_LINE_PATTERN = /^\s*(?:import|export)\s[^;]*?\sfrom\s*['"]([^'"]+)['"]/gm;
const DYNAMIC_IMPORT_PATTERN = /\bimport\(\s*['"]([^'"]+)['"]\s*\)/g;

function findJsFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return findJsFiles(path);
    return entry.name.endsWith('.js') ? [path] : [];
  });
}

describe('npm tarball', () => {
  let installDir: string;

  beforeAll(async () => {
    // Self-contained, same reasoning as the other build-output suites: this only means something
    // against real build output, and nothing upstream guarantees a fresh `dist` exists.
    const { build } = await import('vite');
    await build({ configFile: join(PACKAGE_ROOT, 'vite.config.ts'), logLevel: 'silent' });
    execFileSync('pnpm', ['exec', 'tsc', '-p', 'tsconfig.json', '--emitDeclarationOnly'], { cwd: PACKAGE_ROOT });

    const packOutputDir = mkdtempSync(join(tmpdir(), 'kafka-config-pack-'));
    const packOutput = execFileSync(
      'npm',
      ['pack', '--json', '--pack-destination', packOutputDir, '--ignore-scripts'],
      { cwd: PACKAGE_ROOT, encoding: 'utf8' },
    );
    const [packed] = JSON.parse(packOutput) as { filename: string }[];
    if (packed === undefined) throw new Error('npm pack produced no tarball');
    const tarballPath = join(packOutputDir, packed.filename);

    installDir = mkdtempSync(join(tmpdir(), 'kafka-config-install-'));
    writeFileSync(join(installDir, 'package.json'), JSON.stringify({ name: 'tarball-test', private: true }));
    execFileSync('npm', ['install', tarballPath, '--no-audit', '--no-fund', '--ignore-scripts'], {
      cwd: installDir,
      encoding: 'utf8',
    });

    rmSync(packOutputDir, { recursive: true, force: true });
  }, 120_000);

  afterAll(() => {
    if (installDir !== undefined) rmSync(installDir, { recursive: true, force: true });
  });

  it('ships a dist that only imports node: builtins or its own relative files', () => {
    const distDir = join(installDir, INSTALLED_PACKAGE_DIR, 'dist');
    const jsFiles = findJsFiles(distDir).filter((file) => statSync(file).isFile());

    expect(jsFiles.length).toBeGreaterThan(0);

    for (const file of jsFiles) {
      const source = readFileSync(file, 'utf8');
      const specifiers = [
        ...[...source.matchAll(STATIC_IMPORT_LINE_PATTERN)].map((match) => match[1]),
        ...[...source.matchAll(DYNAMIC_IMPORT_PATTERN)].map((match) => match[1]),
      ];

      for (const specifier of specifiers) {
        expect(
          specifier !== undefined && RELATIVE_OR_NODE_IMPORT_PATTERN.test(specifier),
          `${file} imports "${String(specifier)}", which is neither a node: builtin nor a relative import — this package has no dependencies to allow`,
        ).toBe(true);
      }
    }
  });

  it('installs from the tarball and loads a real config file', async () => {
    const mod: Record<string, unknown> = await import(
      pathToFileURL(join(installDir, INSTALLED_PACKAGE_DIR, 'dist/index.js')).href
    );
    const loadConfigFileSync = mod.loadConfigFileSync as (path: string) => unknown;

    const fixturePath = join(PACKAGE_ROOT, 'test/fixtures/load-sync/ladder/kafka.config.ts');
    expect(loadConfigFileSync(fixturePath)).toEqual({ client: { brokers: ['ts:9092'] } });
  });
});
