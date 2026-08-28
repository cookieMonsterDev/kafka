import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');
const DIST = join(PACKAGE_ROOT, 'dist');

const RELATIVE_IMPORT_PATTERN = /(?:from|import)\s*\(?\s*['"](\.[^'"]+)['"]/g;

/** Every file a JS entry point reaches via static (or dynamic) relative `import`, transitively. */
function walkReachable(entry: string): Set<string> {
  const seen = new Set<string>();
  const queue = [entry];

  while (queue.length > 0) {
    const file = queue.pop();
    if (file === undefined || seen.has(file)) continue;
    seen.add(file);

    const source = readFileSync(file, 'utf8');
    for (const match of source.matchAll(RELATIVE_IMPORT_PATTERN)) {
      const specifier = match[1];
      if (specifier === undefined) continue;
      const resolved = resolve(dirname(file), specifier);
      if (!seen.has(resolved)) queue.push(resolved);
    }
  }

  return seen;
}

describe('the ./config subpath build output', () => {
  beforeAll(async () => {
    // Self-contained: CI's unit job does not run `pnpm build` first, and this assertion is only
    // meaningful against real build output (preserveModules governs what gets emitted at all).
    const { build } = await import('vite');
    await build({ configFile: join(PACKAGE_ROOT, 'vite.config.ts'), logLevel: 'silent' });
  }, 60_000);

  it('emits dist/config/index.js', () => {
    expect(existsSync(join(DIST, 'config', 'index.js'))).toBe(true);
  });

  it('never reaches a dist/config/* file from dist/index.js (reachability, not a regex on the text)', () => {
    const reachable = walkReachable(join(DIST, 'index.js'));
    const configFiles = [...reachable].filter((file) => file.startsWith(join(DIST, 'config') + '/'));

    expect(configFiles).toEqual([]);
  });

  it('imports @cookiemonsterdev/kafka-config as a bare specifier, with no inlined copy', () => {
    const configDir = join(DIST, 'config');
    const sources = readdirSync(configDir)
      .filter((file) => file.endsWith('.js'))
      .map((file) => readFileSync(join(configDir, file), 'utf8'));

    expect(sources.some((source) => /from\s*['"]@cookiemonsterdev\/kafka-config['"]/.test(source))).toBe(true);

    // "No inlined copy": these strings are specific to the dependency's own internals (the
    // transform-hook rescue), not anything core's own config/*.ts files happen to write
    // themselves — present only if vite bundled the dependency's code in instead of leaving it
    // external.
    for (const source of sources) {
      expect(source).not.toContain("mode: 'transform'");
      expect(source).not.toContain('registerHooks');
    }
  });

  it('resolves and exposes the documented config API from a scratch import', async () => {
    const mod: Record<string, unknown> = await import(pathToFileURL(join(DIST, 'config', 'index.js')).href);

    expect(typeof mod.defineConfig).toBe('function');
    expect(typeof mod.loadKafkaConfig).toBe('function');
  });

  it('does not re-export the generic loader machinery — import that from @cookiemonsterdev/kafka-config', async () => {
    const mod: Record<string, unknown> = await import(pathToFileURL(join(DIST, 'config', 'index.js')).href);

    expect(mod.loadConfigFileSync).toBeUndefined();
    expect(mod.loadConfigFileAsync).toBeUndefined();
    expect(mod.discoverConfigFile).toBeUndefined();
    expect(mod.mergeConfigLayers).toBeUndefined();
    expect(mod.KafkaConfigError).toBeUndefined();
    expect(mod.KafkaConfigRequiresAsyncError).toBeUndefined();
  });
});
