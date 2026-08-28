import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../../');
const DIST = join(PACKAGE_ROOT, 'dist');
const SRC = join(PACKAGE_ROOT, 'src');
const DRIVER = join(PACKAGE_ROOT, 'test/helpers/run-duplicate-instances.mjs');
const FIXTURES = join(PACKAGE_ROOT, 'test/fixtures');

interface DriverResult {
  copiesAreDistinctModuleInstances: boolean;
  deepEqualAcrossCopies: boolean;
  errorFromAHasKafkaConfigErrorName: boolean;
  errorFromAIsNotInstanceOfCopyBClass: boolean;
  copyBOwnErrorHasSameName: boolean;
  copyBLoadsRescuableFixtureAfterCopyAInstalledHooks: boolean;
  rescuedConfig?: unknown;
  rescueError?: string;
}

/**
 * D18a rests on duplication across two copies of this loader being harmless. This is what proves
 * it — never just assumed — by loading the built package twice from two distinct resolved paths
 * (the original `dist/` and a filesystem copy of it, so Node's module registry, keyed by resolved
 * specifier, creates two genuinely separate instances) inside one subprocess.
 */
describe('two copies of the loader are harmless (D18a, Risk #15)', () => {
  let copyBRoot: string;

  beforeAll(async () => {
    // Self-contained, same reasoning as core's build-output.test.ts: this assertion is only
    // meaningful against real build output, and nothing upstream guarantees `dist` exists yet.
    const { build } = await import('vite');
    await build({ configFile: join(PACKAGE_ROOT, 'vite.config.ts'), logLevel: 'silent' });

    copyBRoot = mkdtempSync(join(tmpdir(), 'kafka-config-copy-b-'));
    cpSync(DIST, copyBRoot, { recursive: true });
  }, 60_000);

  afterAll(() => {
    if (copyBRoot !== undefined) rmSync(copyBRoot, { recursive: true, force: true });
  });

  it('proves the invariant dynamically, across two distinct resolved paths', () => {
    const output = execFileSync(
      'node',
      [
        DRIVER,
        join(DIST, 'index.js'),
        join(copyBRoot, 'index.js'),
        join(FIXTURES, 'load-sync/ladder/kafka.config.ts'),
        join(FIXTURES, 'load-sync/invalid-json/kafka.config.json'),
        join(FIXTURES, 'transform-hooks/enum/kafka.config.ts'),
      ],
      { encoding: 'utf8' },
    );
    const result = JSON.parse(output) as DriverResult;

    expect(result.copiesAreDistinctModuleInstances).toBe(true);
    expect(result.deepEqualAcrossCopies).toBe(true);
    expect(result.errorFromAHasKafkaConfigErrorName).toBe(true);
    expect(result.errorFromAIsNotInstanceOfCopyBClass).toBe(true);
    expect(result.copyBOwnErrorHasSameName).toBe(true);
    expect(result.copyBLoadsRescuableFixtureAfterCopyAInstalledHooks).toBe(true);
    expect(result.rescuedConfig).toEqual({ client: { brokers: ['enum:info'] } });
  });

  it('never brands its own errors with Symbol', () => {
    const files = readdirSync(SRC).filter((file) => file.endsWith('.ts') && !file.endsWith('.test.ts'));

    for (const file of files) {
      const source = readFileSync(join(SRC, file), 'utf8');
      expect(source, `${file} must not use a Symbol-based brand check`).not.toMatch(/\bSymbol\(/);
    }
  });

  it(
    "the only instanceof checks against this package's own error classes stay inside load-sync.ts, " +
      'catching an error it just threw in the same call — never a value that crossed a copy boundary',
    () => {
      const files = readdirSync(SRC).filter((file) => file.endsWith('.ts') && !file.endsWith('.test.ts'));
      const ownClassInstanceofPattern = /instanceof (KafkaConfigError|KafkaConfigRequiresAsyncError)\b/;

      const filesWithOwnClassInstanceof = files
        .filter((file) => ownClassInstanceofPattern.test(readFileSync(join(SRC, file), 'utf8')))
        .sort();

      expect(filesWithOwnClassInstanceof).toEqual(['load-sync.ts']);
    },
  );
});
