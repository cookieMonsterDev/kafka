import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { initCommand } from './init';

// Scaffolded under the package root (not the system tmpdir) so the generated file's
// `import ... from '@cookiemonsterdev/kafka-core'` resolves the same way it would in a real
// project — Node's bare-specifier resolution walks up through ancestor `node_modules`, which
// only exists inside this workspace.
const PACKAGE_ROOT = join(dirname(fileURLToPath(import.meta.url)), '../../..');

let dir: string;

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

function makeDir(): string {
  dir = mkdtempSync(join(PACKAGE_ROOT, '.tmp-init-'));
  return dir;
}

describe('initCommand', () => {
  it('scaffolds kafka.config.mjs by default in a directory with no TypeScript signal', async () => {
    const cwd = makeDir();
    const { context } = createFakeCommandContext({ cwd });

    const code = await initCommand.run(context);

    expect(code).toBe(0);
    expect(existsSync(join(cwd, 'kafka.config.mjs'))).toBe(true);
  });

  it('scaffolds kafka.config.ts when a tsconfig.json is present', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'tsconfig.json'), '{}');
    const { context } = createFakeCommandContext({ cwd });

    await initCommand.run(context);

    expect(existsSync(join(cwd, 'kafka.config.ts'))).toBe(true);
  });

  it('scaffolds kafka.config.ts when typescript is a package.json dependency', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'package.json'), JSON.stringify({ devDependencies: { typescript: '^5.0.0' } }));
    const { context } = createFakeCommandContext({ cwd });

    await initCommand.run(context);

    expect(existsSync(join(cwd, 'kafka.config.ts'))).toBe(true);
  });

  it('--js overrides TypeScript auto-detection', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'tsconfig.json'), '{}');
    const { context } = createFakeCommandContext({ cwd, flags: { js: true } });

    await initCommand.run(context);

    expect(existsSync(join(cwd, 'kafka.config.mjs'))).toBe(true);
    expect(existsSync(join(cwd, 'kafka.config.ts'))).toBe(false);
  });

  it('--ts forces TypeScript even with no signal', async () => {
    const cwd = makeDir();
    const { context } = createFakeCommandContext({ cwd, flags: { ts: true } });

    await initCommand.run(context);

    expect(existsSync(join(cwd, 'kafka.config.ts'))).toBe(true);
  });

  it('rejects --ts and --js together', async () => {
    const cwd = makeDir();
    const { context } = createFakeCommandContext({ cwd, flags: { ts: true, js: true } });

    await expect(initCommand.run(context)).rejects.toThrow(/mutually exclusive/);
  });

  it('refuses to overwrite an existing file without --force', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'kafka.config.mjs'), 'export default {};\n');
    const { context } = createFakeCommandContext({ cwd });

    await expect(initCommand.run(context)).rejects.toThrow(/already exists/);
  });

  it('overwrites an existing file with --force', async () => {
    const cwd = makeDir();
    const target = join(cwd, 'kafka.config.mjs');
    writeFileSync(target, 'stale content');
    const { context } = createFakeCommandContext({ cwd, flags: { force: true } });

    await initCommand.run(context);

    expect(readFileSync(target, 'utf8')).toContain('brokers');
  });

  // These two go through the real dynamic loader (Node's `import()`/`createRequire()`, including
  // its TypeScript-stripping retry path), not a fake — real filesystem I/O plus a real module
  // load, occasionally slow under CI's shared, resource-constrained runners. The default 5s unit
  // timeout is fine locally but has been observed to trip in CI, so these get a longer one.
  it('emits a kafka.config.mjs that imports cleanly via the real loader', async () => {
    const cwd = makeDir();
    const { context } = createFakeCommandContext({ cwd });

    await initCommand.run(context);

    const { loadConfigFileAsync } = await import('@cookiemonsterdev/kafka-config');
    const loaded = await loadConfigFileAsync(join(cwd, 'kafka.config.mjs'));
    expect(loaded).toEqual({ client: { brokers: ['localhost:9092'] } });
  }, 15_000);

  it('emits a kafka.config.ts that the real (core-typed) loader accepts', async () => {
    const cwd = makeDir();
    const { context } = createFakeCommandContext({ cwd, flags: { ts: true } });

    await initCommand.run(context);

    const { loadKafkaConfig } = await import('@cookiemonsterdev/kafka-core');
    const loaded = loadKafkaConfig(join(cwd, 'kafka.config.ts'));
    expect(loaded).toEqual({ client: { brokers: ['localhost:9092'] } });
  }, 15_000);
});
