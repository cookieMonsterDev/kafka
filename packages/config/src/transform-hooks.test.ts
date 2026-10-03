import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

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
}

/**
 * Runs the loader against one config file in a brand-new process. `module.registerHooks` has no
 * `deregister` on this Node version, so registering it inside the shared vitest worker would
 * silently change `.ts` resolution semantics for every test that runs after it.
 */
function runLoadSync(configPath: string, allowTransformFallback = true): DriverResult {
  const output = execFileSync('node', [DRIVER, configPath, String(allowTransformFallback)], {
    encoding: 'utf8',
  });
  return JSON.parse(output) as DriverResult;
}

describe('transform-hook fallback (subprocess)', () => {
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

  it('takes no hooks and emits no diagnostics on the happy path', () => {
    const path = join(FIXTURES, 'load-sync/ladder/kafka.config.ts');

    const result = runLoadSync(path);

    expect(result.ok).toBe(true);
    expect(result.hooksInstalled).toBe(false);
    expect(result.diagnostics).toEqual([]);
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
