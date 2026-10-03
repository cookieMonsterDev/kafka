import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { isBun } from './runtime';

const HERE = dirname(fileURLToPath(import.meta.url));
const FIXTURES = join(HERE, '../test/fixtures');
const DRIVER = join(HERE, '../test/helpers/run-load-sync.mjs');

interface DriverResult {
  ok: boolean;
  config?: unknown;
  name?: string;
  tag?: string;
  message?: string;
  diagnostics: { code: string; level: string; message: string; detail?: string; fix?: string }[];
  hooksInstalled: boolean;
  runtime: 'node' | 'bun';
}

/**
 * Runs the loader against one config file in a brand-new process. `module.registerHooks` has no
 * `deregister` on this Node version, so registering it inside the shared vitest worker would
 * silently change `.ts` resolution semantics for every test that runs after it.
 *
 * Spawns `process.execPath`, never a bare `node`: `bun --bun <bin>` (like `bun run --bun`) puts a
 * `node` -> Bun shim first on PATH, so `node` may not be the runtime these assertions are about.
 */
function runLoadSync(configPath: string, allowTransformFallback = true): DriverResult {
  const output = execFileSync(process.execPath, [DRIVER, configPath, String(allowTransformFallback)], {
    encoding: 'utf8',
  });
  return JSON.parse(output) as DriverResult;
}

describe('transform-hook fallback (subprocess)', () => {
  it('runs the driver on the same runtime as the test', () => {
    const result = runLoadSync(join(FIXTURES, 'load-sync/ladder/kafka.config.ts'));

    expect(result.runtime).toBe(isBun() ? 'bun' : 'node');
  });

  it('takes no hooks and emits no diagnostics on the happy path', () => {
    const path = join(FIXTURES, 'load-sync/ladder/kafka.config.ts');

    const result = runLoadSync(path);

    expect(result.ok).toBe(true);
    expect(result.hooksInstalled).toBe(false);
    expect(result.diagnostics).toEqual([]);
  });

  it.runIf(isBun()).each([
    ['an enum', 'transform-hooks/enum/kafka.config.ts', { client: { brokers: ['enum:info'] } }],
    [
      'an extensionless import',
      'transform-hooks/extensionless/kafka.config.ts',
      { client: { brokers: ['extensionless:9092'] } },
    ],
    [
      'an enum behind an extensionless import',
      'transform-hooks/extensionless-enum/kafka.config.ts',
      { client: { brokers: ['extensionless-enum:9092'] } },
    ],
  ])('on Bun, loads %s natively with no hooks or diagnostics', (_label, rel, expected) => {
    for (const allowTransformFallback of [true, false]) {
      const result = runLoadSync(join(FIXTURES, rel), allowTransformFallback);

      expect(result.ok).toBe(true);
      expect(result.config).toEqual(expected);
      expect(result.hooksInstalled).toBe(false);
      expect(result.diagnostics).toEqual([]);
    }
  });
});

// `module.registerHooks` rescue paths: Node only. Bun loads these fixtures natively (see above).
describe.skipIf(isBun())('transform-hook fallback on Node (subprocess)', () => {
  it.each([true, false])(
    'never rescues a TS enum (allowTransformFallback: %s): fails naming the fix, installs no hooks',
    (allowTransformFallback) => {
      const path = join(FIXTURES, 'transform-hooks/enum/kafka.config.ts');

      const result = runLoadSync(path, allowTransformFallback);

      expect(result.ok).toBe(false);
      expect(result.name).toBe('KafkaConfigError');
      expect(result.tag).toBe('ConfigLoadError');
      expect(result.message).toContain('enum');
      expect(result.message).toContain('frozen object');
      expect(result.message).not.toContain('allowTransformFallback');
      expect(result.hooksInstalled).toBe(false);
      expect(result.diagnostics).toEqual([]);
    },
  );

  it('surfaces the same enum error when the enum is only reached through a rescued extensionless import', () => {
    const path = join(FIXTURES, 'transform-hooks/extensionless-enum/kafka.config.ts');

    const result = runLoadSync(path);

    expect(result.ok).toBe(false);
    expect(result.name).toBe('KafkaConfigError');
    expect(result.tag).toBe('ConfigLoadError');
    expect(result.message).toContain('frozen object');
    expect(result.hooksInstalled).toBe(true);
    expect(result.diagnostics).toHaveLength(1);
    expect(result.diagnostics[0]).toMatchObject({
      code: 'config.transform-fallback',
      detail: expect.stringContaining('extension'),
    });
  });

  it('rescues an extensionless relative import, installs hooks, and warns naming the fix', () => {
    const path = join(FIXTURES, 'transform-hooks/extensionless/kafka.config.ts');

    const result = runLoadSync(path);

    expect(result.ok).toBe(true);
    expect(result.config).toEqual({ client: { brokers: ['extensionless:9092'] } });
    expect(result.hooksInstalled).toBe(true);
    expect(result.diagnostics).toHaveLength(1);
    expect(result.diagnostics[0]).toMatchObject({
      code: 'config.transform-fallback',
      level: 'warn',
      detail: expect.stringContaining('extension'),
      fix: expect.stringContaining('.ts'),
    });
  });

  it.each([['an extensionless import', 'transform-hooks/extensionless/kafka.config.ts', '.ts']])(
    'allowTransformFallback: false surfaces a rewritten error for %s and never installs hooks',
    (_label, rel, fixNeedle) => {
      const path = join(FIXTURES, rel);

      const result = runLoadSync(path, false);

      expect(result.ok).toBe(false);
      expect(result.name).toBe('KafkaConfigError');
      expect(result.tag).toBe('ConfigLoadError');
      expect(result.message).toContain('allowTransformFallback');
      expect(result.message).toContain(fixNeedle);
      expect(result.hooksInstalled).toBe(false);
      expect(result.diagnostics).toEqual([]);
    },
  );
});
