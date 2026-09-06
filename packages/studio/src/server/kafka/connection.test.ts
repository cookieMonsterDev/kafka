import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createKafkaClient,
  isKnownProfile,
  listProfileNames,
  resolveStudioConnectionConfig,
  UnknownProfileError,
} from './connection';

describe('resolveStudioConnectionConfig', () => {
  it('returns no profiles when no config file is found', async () => {
    const config = await resolveStudioConnectionConfig({ cwd: '/nonexistent-test-cwd', env: {} });
    expect(config.path).toBeNull();
    expect(config.fileConfig).toBeNull();
    expect(config.profiles).toEqual({});
  });

  it('throws when KAFKA_CONFIG points at a file that does not exist', async () => {
    await expect(
      resolveStudioConnectionConfig({ cwd: '/nonexistent-test-cwd', env: { KAFKA_CONFIG: 'nope.json' } }),
    ).rejects.toThrow('does not exist');
  });

  describe('with a real config file', () => {
    let dir: string;

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'studio-connection-'));
      writeFileSync(
        join(dir, 'kafka.config.json'),
        JSON.stringify({
          client: { brokers: ['file:9092'] },
          cli: { profiles: { staging: { brokers: ['staging:9092'] }, bogus: 'not-an-object' } },
        }),
      );
    });

    afterEach(() => {
      rmSync(dir, { recursive: true, force: true });
    });

    it('reads profiles from the "cli.profiles" section, skipping malformed entries', async () => {
      const config = await resolveStudioConnectionConfig({ cwd: dir, env: {} });
      expect(config.path).toBe(join(dir, 'kafka.config.json'));
      expect(listProfileNames(config)).toEqual(['staging']);
      expect(isKnownProfile(config, 'staging')).toBe(true);
      expect(isKnownProfile(config, 'bogus')).toBe(false);
      expect(isKnownProfile(config, 'nope')).toBe(false);
    });

    it('warns about a malformed profile entry instead of silently dropping it', async () => {
      const onWarn = vi.fn();
      await resolveStudioConnectionConfig({ cwd: dir, env: {}, onWarn });
      expect(onWarn).toHaveBeenCalledWith(expect.stringContaining('"cli.profiles.bogus"'));
    });

    it('createKafkaClient(null) falls through to the file\'s "client" section', async () => {
      const config = await resolveStudioConnectionConfig({ cwd: dir, env: {} });
      const kafka = createKafkaClient(config, null);
      expect(kafka.configSource().keys.brokers).toBe('file');
    });

    it('createKafkaClient(profile) layers the named profile over the file', async () => {
      const config = await resolveStudioConnectionConfig({ cwd: dir, env: {} });
      const kafka = createKafkaClient(config, 'staging');
      expect(kafka.configSource().keys.brokers).toBe('explicit');
    });

    it('createKafkaClient throws UnknownProfileError for an unconfigured profile', async () => {
      const config = await resolveStudioConnectionConfig({ cwd: dir, env: {} });
      let caught: unknown;
      try {
        createKafkaClient(config, 'nope');
      } catch (error) {
        caught = error;
      }
      expect(caught).toBeInstanceOf(UnknownProfileError);
      expect((caught as UnknownProfileError).available).toEqual(['staging']);
    });
  });
});
