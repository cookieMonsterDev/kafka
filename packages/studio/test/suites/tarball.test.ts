import { execFileSync } from 'node:child_process';
import { chmodSync, cpSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');
const INSTALLED_PACKAGE_DIR = 'node_modules/@cookiemonsterdev/kafka-studio';

const RELATIVE_OR_NODE_IMPORT_PATTERN = /^(?:\.\.?\/|node:)/;
const DECLARED_DEPENDENCIES = ['@cookiemonsterdev/kafka-core', '@cookiemonsterdev/kafka-config'];
// `vite` is a devDependency, deliberately excluded from `dependencies` (see vite.config.ts) — it
// must only ever reach a dynamic `import('vite')` (server/dev.ts, gated behind
// KAFKA_STUDIO_DEV=1), never a static import that Node would resolve eagerly on every install.
const DYNAMIC_ONLY_DEPENDENCIES = ['vite'];
const STATIC_IMPORT_LINE_PATTERN = /^\s*(?:import|export)\s[^;]*?\sfrom\s*['"]([^'"]+)['"]/gm;
const DYNAMIC_IMPORT_PATTERN = /\bimport\(\s*['"]([^'"]+)['"]\s*\)/g;

function findJsFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return findJsFiles(path);
    return entry.name.endsWith('.js') ? [path] : [];
  });
}

function isAllowedStaticSpecifier(specifier: string): boolean {
  return RELATIVE_OR_NODE_IMPORT_PATTERN.test(specifier) || DECLARED_DEPENDENCIES.includes(specifier);
}

function isAllowedDynamicSpecifier(specifier: string): boolean {
  return isAllowedStaticSpecifier(specifier) || DYNAMIC_ONLY_DEPENDENCIES.includes(specifier);
}

describe('npm tarball', () => {
  let installDir: string;
  let binPath: string;

  beforeAll(async () => {
    // Self-contained: build into an isolated tree, never the package's own `dist/`, so this
    // suite's build cannot race a sibling suite still importing the real dist.
    const packSource = mkdtempSync(join(tmpdir(), 'kafka-studio-pack-source-'));
    try {
      const distDir = join(packSource, 'dist');

      const { build } = await import('vite');
      await build({
        configFile: join(PACKAGE_ROOT, 'vite.config.ts'),
        logLevel: 'silent',
        build: { outDir: distDir, emptyOutDir: true },
      });
      await build({
        configFile: join(PACKAGE_ROOT, 'vite.web.config.ts'),
        logLevel: 'silent',
        build: { outDir: join(distDir, 'web'), emptyOutDir: true },
      });
      execFileSync('pnpm', ['exec', 'tsc', '-p', 'tsconfig.json', '--emitDeclarationOnly', '--outDir', distDir], {
        cwd: PACKAGE_ROOT,
      });
      // npm preserves whatever mode is in the tarball; chmod explicitly rather than relying on
      // npm's own install-time bin chmod, so this suite's `execFileSync` doesn't depend on it.
      chmodSync(join(distDir, 'bin.js'), 0o755);
      cpSync(join(PACKAGE_ROOT, 'package.json'), join(packSource, 'package.json'));
      cpSync(join(PACKAGE_ROOT, 'README.md'), join(packSource, 'README.md'));

      const packOutputDir = mkdtempSync(join(tmpdir(), 'kafka-studio-pack-'));
      const packOutput = execFileSync(
        'npm',
        ['pack', '--json', '--pack-destination', packOutputDir, '--ignore-scripts'],
        { cwd: packSource, encoding: 'utf8' },
      );
      const [packed] = JSON.parse(packOutput) as { filename: string }[];
      if (packed === undefined) throw new Error('npm pack produced no tarball');
      const tarballPath = join(packOutputDir, packed.filename);

      installDir = mkdtempSync(join(tmpdir(), 'kafka-studio-install-'));
      writeFileSync(join(installDir, 'package.json'), JSON.stringify({ name: 'tarball-test', private: true }));
      execFileSync('npm', ['install', tarballPath, '--no-audit', '--no-fund', '--ignore-scripts'], {
        cwd: installDir,
        encoding: 'utf8',
      });

      rmSync(packOutputDir, { recursive: true, force: true });
    } finally {
      rmSync(packSource, { recursive: true, force: true });
    }

    binPath = join(installDir, INSTALLED_PACKAGE_DIR, 'dist/bin.js');
  }, 300_000);

  afterAll(() => {
    if (installDir !== undefined) rmSync(installDir, { recursive: true, force: true });
  });

  it('ships a dist that only imports node: builtins, its own relative files, or a declared dependency', () => {
    const distDir = join(installDir, INSTALLED_PACKAGE_DIR, 'dist');
    const jsFiles = findJsFiles(distDir).filter((file) => statSync(file).isFile());

    expect(jsFiles.length).toBeGreaterThan(0);

    for (const file of jsFiles) {
      const source = readFileSync(file, 'utf8');
      const staticSpecifiers = [...source.matchAll(STATIC_IMPORT_LINE_PATTERN)].map((match) => match[1]);
      const dynamicSpecifiers = [...source.matchAll(DYNAMIC_IMPORT_PATTERN)].map((match) => match[1]);

      for (const specifier of staticSpecifiers) {
        expect(
          specifier !== undefined && isAllowedStaticSpecifier(specifier),
          `${file} statically imports "${String(specifier)}", which is neither node:, a relative import, nor a declared dependency`,
        ).toBe(true);
      }

      for (const specifier of dynamicSpecifiers) {
        expect(
          specifier !== undefined && isAllowedDynamicSpecifier(specifier),
          `${file} dynamically imports "${String(specifier)}", which is neither node:, a relative import, a declared dependency, nor an allowed dynamic-only dependency`,
        ).toBe(true);
      }
    }
  });

  it('ships the built web spa alongside the server bundle', () => {
    const webIndex = join(installDir, INSTALLED_PACKAGE_DIR, 'dist/web/index.html');
    expect(statSync(webIndex).isFile()).toBe(true);
  });

  it('runs kafka-studio --version from the installed bin', () => {
    const output = execFileSync('node', [binPath, '--version'], { encoding: 'utf8' });
    expect(output.trim()).toMatch(/^\d+\.\d+\.\d+/);
  });

  it('runs kafka-studio --help from the installed bin', () => {
    const output = execFileSync('node', [binPath, '--help'], { encoding: 'utf8' });
    expect(output).toContain('Usage: kafka-studio [options]');
  });
});
