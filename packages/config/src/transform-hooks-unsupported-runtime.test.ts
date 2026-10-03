import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it, vi } from 'vitest';
import type * as NodeModule from 'node:module';
import { loadConfigFileSync } from './load-sync';
import { isBun } from './runtime';
import { areConfigTransformHooksInstalled, installConfigTransformHooks } from './transform-hooks';

// Simulates Bun's `node:module`, which has neither `registerHooks` nor `stripTypeScriptTypes`.
// (Set to `undefined` rather than omitted: vitest throws on reading an export a mock factory left
// out, while a real runtime just yields `undefined`.)
vi.mock('node:module', async (importOriginal) => {
  const actual = await importOriginal<typeof NodeModule>();
  return { ...actual, registerHooks: undefined, stripTypeScriptTypes: undefined };
});

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '../test/fixtures');

describe('transform hooks on a runtime without registerHooks', () => {
  it('installConfigTransformHooks is a no-op that reports nothing was installed', () => {
    expect(installConfigTransformHooks()).toBe(false);
    expect(areConfigTransformHooksInstalled()).toBe(false);
  });

  it.skipIf(isBun())(
    'the sync loader fails a rescuable file with an error naming the fix, never claiming a rescue',
    () => {
      const path = join(FIXTURES, 'transform-hooks/extensionless/kafka.config.ts');
      const diagnostics: string[] = [];

      expect(() => loadConfigFileSync(path, { onDiagnostic: (d) => diagnostics.push(d.code) })).toThrow(
        expect.objectContaining({
          name: 'KafkaConfigError',
          tag: 'ConfigLoadError',
          message: expect.stringMatching(/does not support.*add the "\.ts"/),
        }),
      );
      expect(diagnostics).toEqual([]);
      expect(areConfigTransformHooksInstalled()).toBe(false);
    },
  );

  it.runIf(isBun())('on Bun, the sync loader loads that same file natively, with no hooks', () => {
    const path = join(FIXTURES, 'transform-hooks/extensionless/kafka.config.ts');
    const diagnostics: string[] = [];

    const config = loadConfigFileSync(path, { onDiagnostic: (d) => diagnostics.push(d.code) });

    expect(config).toEqual({ client: { brokers: ['extensionless:9092'] } });
    expect(diagnostics).toEqual([]);
    expect(areConfigTransformHooksInstalled()).toBe(false);
  });
});
