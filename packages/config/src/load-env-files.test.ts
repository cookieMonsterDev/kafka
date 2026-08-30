import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';

const HERE = dirname(fileURLToPath(import.meta.url));
const DRIVER = join(HERE, '../test/helpers/run-load-env-files.mjs');

interface DriverResult {
  loaded: string[];
  missing: string[];
  env: Record<string, string | undefined>;
}

let dir: string | undefined;

afterEach(() => {
  if (dir != null) {
    rmSync(dir, { recursive: true, force: true });
    dir = undefined;
  }
});

function tempDir(): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-config-load-env-files-'));
  return dir;
}

/** Runs `loadEnvFiles` in a brand-new process — `process.loadEnvFile` mutates real process env with no undo. */
function runLoadEnvFiles(cwd: string, files: string[], processEnv: Record<string, string> = {}): DriverResult {
  const output = execFileSync('node', [DRIVER, cwd, ...files], {
    encoding: 'utf8',
    env: { ...process.env, ...processEnv },
  });
  return JSON.parse(output) as DriverResult;
}

describe('loadEnvFiles (subprocess)', () => {
  it('loads a single .env file from an explicit directory', () => {
    const cwd = tempDir();
    writeFileSync(join(cwd, '.env'), 'KAFKA_CONFIG_TEST_A=from-file\n');

    const result = runLoadEnvFiles(cwd, ['.env']);

    expect(result.missing).toEqual([]);
    expect(result.loaded).toHaveLength(1);
    expect(result.loaded[0]).toContain('.env');
    expect(result.env.KAFKA_CONFIG_TEST_A).toBe('from-file');
  });

  it('does not override an already-set variable', () => {
    const cwd = tempDir();
    writeFileSync(join(cwd, '.env'), 'KAFKA_CONFIG_TEST_A=from-file\n');

    const result = runLoadEnvFiles(cwd, ['.env'], { KAFKA_CONFIG_TEST_A: 'from-process' });

    expect(result.loaded).toHaveLength(1);
    expect(result.env.KAFKA_CONFIG_TEST_A).toBe('from-process');
  });

  it('reports a missing file in `missing`, never a throw', () => {
    const cwd = tempDir();

    const result = runLoadEnvFiles(cwd, ['.env']);

    expect(result.loaded).toEqual([]);
    expect(result.missing).toHaveLength(1);
    expect(result.missing[0]).toContain('.env');
  });

  it('loads multiple files in order — an earlier file wins a key both define', () => {
    const cwd = tempDir();
    writeFileSync(join(cwd, 'first.env'), 'KAFKA_CONFIG_TEST_A=first\nKAFKA_CONFIG_TEST_B=only-first\n');
    writeFileSync(join(cwd, 'second.env'), 'KAFKA_CONFIG_TEST_A=second\nKAFKA_CONFIG_TEST_C=only-second\n');

    const result = runLoadEnvFiles(cwd, ['first.env', 'second.env']);

    expect(result.loaded).toHaveLength(2);
    expect(result.env.KAFKA_CONFIG_TEST_A).toBe('first');
    expect(result.env.KAFKA_CONFIG_TEST_B).toBe('only-first');
    expect(result.env.KAFKA_CONFIG_TEST_C).toBe('only-second');
  });

  it('reports a mix of loaded and missing files without stopping early', () => {
    const cwd = tempDir();
    writeFileSync(join(cwd, 'present.env'), 'KAFKA_CONFIG_TEST_A=present\n');

    const result = runLoadEnvFiles(cwd, ['present.env', 'absent.env']);

    expect(result.loaded).toHaveLength(1);
    expect(result.missing).toHaveLength(1);
    expect(result.missing[0]).toContain('absent.env');
  });

  it('defaults to [".env"] when no files are given', () => {
    const cwd = tempDir();
    writeFileSync(join(cwd, '.env'), 'KAFKA_CONFIG_TEST_A=default-file\n');

    const result = runLoadEnvFiles(cwd, []);

    expect(result.loaded).toHaveLength(1);
    expect(result.env.KAFKA_CONFIG_TEST_A).toBe('default-file');
  });
});
