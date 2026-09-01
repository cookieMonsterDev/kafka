import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CliConfigError } from '../errors/cli-config-error';
import { resolveCliConfig } from './resolve';

let dir: string;

function makeDir(): string {
  dir = mkdtempSync(join(tmpdir(), 'kafka-cli-resolve-'));
  // A `.git` marker bounds discovery's upward search to this directory, so "no config found"
  // resolves deterministically instead of walking toward the real filesystem root.
  mkdirSync(join(dir, '.git'));
  return dir;
}

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe('resolveCliConfig', () => {
  it('resolves to no config file when none exists', async () => {
    const cwd = makeDir();
    const resolved = await resolveCliConfig({ cwd, env: {} });

    expect(resolved).toEqual({
      path: null,
      fileConfig: null,
      cli: {},
      profile: null,
      transformFallbackUsed: false,
    });
  });

  it('discovers and loads a kafka.config.mjs in cwd', async () => {
    const cwd = makeDir();
    const configPath = join(cwd, 'kafka.config.mjs');
    writeFileSync(configPath, 'export default { client: { brokers: ["a:1"] } };\n');

    const onDiagnostic = vi.fn();
    const resolved = await resolveCliConfig({ cwd, env: {}, onDiagnostic });

    expect(resolved.path).toBe(configPath);
    expect(resolved.fileConfig).toEqual({ client: { brokers: ['a:1'] } });
    expect(onDiagnostic).toHaveBeenCalledWith(
      expect.objectContaining({ code: 'config.loaded', level: 'info', path: configPath }),
    );
  });

  it('reads the cli: section, forwarding unknown keys to onWarn', async () => {
    const cwd = makeDir();
    writeFileSync(
      join(cwd, 'kafka.config.mjs'),
      'export default { client: { brokers: ["a:1"] }, cli: { output: "json", bogus: true } };\n',
    );
    const onWarn = vi.fn();

    const resolved = await resolveCliConfig({ cwd, env: {}, onWarn });

    expect(resolved.cli).toEqual({ output: 'json' });
    expect(onWarn).toHaveBeenCalledWith(expect.stringContaining('cli.bogus'));
  });

  it('prefers an explicit --config-file path over discovery', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'kafka.config.mjs'), 'export default { client: { brokers: ["discovered:1"] } };\n');
    const explicitDir = mkdtempSync(join(tmpdir(), 'kafka-cli-resolve-explicit-'));
    const explicitPath = join(explicitDir, 'other.config.mjs');
    writeFileSync(explicitPath, 'export default { client: { brokers: ["explicit:1"] } };\n');

    try {
      const resolved = await resolveCliConfig({ cwd, env: {}, configFlag: explicitPath });
      expect(resolved.path).toBe(explicitPath);
      expect(resolved.fileConfig).toEqual({ client: { brokers: ['explicit:1'] } });
    } finally {
      rmSync(explicitDir, { recursive: true, force: true });
    }
  });

  it('throws CliConfigError when --config-file does not exist', async () => {
    const cwd = makeDir();
    await expect(resolveCliConfig({ cwd, env: {}, configFlag: './nope.config.mjs' })).rejects.toThrow(CliConfigError);
  });

  it('falls back to KAFKA_CONFIG when --config-file is not given', async () => {
    const cwd = makeDir();
    const configPath = join(cwd, 'kafka.config.mjs');
    writeFileSync(configPath, 'export default { client: { brokers: ["a:1"] } };\n');

    const resolved = await resolveCliConfig({ cwd, env: { KAFKA_CONFIG: configPath } });
    expect(resolved.path).toBe(configPath);
  });

  it('resolves the active profile from --profile', async () => {
    const cwd = makeDir();
    writeFileSync(
      join(cwd, 'kafka.config.mjs'),
      'export default { cli: { profiles: { staging: { brokers: ["s:1"] } } } };\n',
    );

    const resolved = await resolveCliConfig({ cwd, env: {}, profileFlag: 'staging' });
    expect(resolved.profile).toBe('staging');
  });

  it('resolves the active profile from KAFKA_PROFILE when --profile is not given', async () => {
    const cwd = makeDir();
    writeFileSync(
      join(cwd, 'kafka.config.mjs'),
      'export default { cli: { profiles: { staging: { brokers: ["s:1"] } } } };\n',
    );

    const resolved = await resolveCliConfig({ cwd, env: { KAFKA_PROFILE: 'staging' } });
    expect(resolved.profile).toBe('staging');
  });

  it('throws CliConfigError naming the available profiles when --profile is unknown', async () => {
    const cwd = makeDir();
    writeFileSync(
      join(cwd, 'kafka.config.mjs'),
      'export default { cli: { profiles: { staging: {}, production: {} } } };\n',
    );

    await expect(resolveCliConfig({ cwd, env: {}, profileFlag: 'bogus' })).rejects.toThrow(
      /unknown profile "bogus".*staging, production/,
    );
  });

  it('throws CliConfigError naming that none are configured when no cli.profiles section exists', async () => {
    const cwd = makeDir();
    await expect(resolveCliConfig({ cwd, env: {}, profileFlag: 'bogus' })).rejects.toThrow(/none configured/);
  });

  it('retries through the async loader for a config that requires top-level await', async () => {
    const cwd = makeDir();
    const configPath = join(cwd, 'kafka.config.mjs');
    writeFileSync(configPath, 'export default { client: { brokers: [await Promise.resolve("a:1")] } };\n');

    const resolved = await resolveCliConfig({ cwd, env: {} });
    expect(resolved.fileConfig).toEqual({ client: { brokers: ['a:1'] } });
  });

  it('wraps a generic loader failure (e.g. invalid JSON) into a CliConfigError', async () => {
    const cwd = makeDir();
    writeFileSync(join(cwd, 'kafka.config.json'), '{ not valid json');

    await expect(resolveCliConfig({ cwd, env: {} })).rejects.toThrow(CliConfigError);
  });
});
