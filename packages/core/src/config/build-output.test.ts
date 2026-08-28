import { existsSync, readFileSync } from 'node:fs';
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
    const source = readFileSync(join(DIST, 'config', 'index.js'), 'utf8');

    expect(source).toMatch(/from\s*['"]@cookiemonsterdev\/kafka-config['"]/);

    // "No inlined copy": none of the machinery's own source strings (present only if vite bundled
    // the dependency's code into this file instead of leaving it external) show up here.
    expect(source).not.toContain('kafka config file');
    expect(source).not.toContain('registerHooks');
  });

  it('resolves and exposes the documented config API from a scratch import', async () => {
    const mod: Record<string, unknown> = await import(pathToFileURL(join(DIST, 'config', 'index.js')).href);

    expect(typeof mod.defineConfig).toBe('function');
    expect(typeof mod.loadKafkaConfig).toBe('function');
    expect(typeof mod.loadConfigFileSync).toBe('function');
    expect(typeof mod.loadConfigFileAsync).toBe('function');
    expect(typeof mod.discoverConfigFile).toBe('function');
    expect(typeof mod.mergeConfigLayers).toBe('function');
    expect(typeof mod.KafkaConfigError).toBe('function');
    expect(typeof mod.KafkaConfigRequiresAsyncError).toBe('function');
  });
});
