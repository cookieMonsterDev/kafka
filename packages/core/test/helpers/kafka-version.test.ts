import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  compareKafkaVersions,
  DEFAULT_KAFKA_VERSION,
  getKafkaVersion,
  isKRaftKafkaVersion,
  isZooKeeperComposeFile,
  KAFKA_VERSION_COMPOSE_FILES,
  kafkaVersionAtLeast,
  kafkaVersionAtMost,
  kafkaVersionEquals,
  normalizeKafkaVersion,
  resolveComposeFile,
} from './kafka-version';

const resolveComposeFileScript = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '../../scripts/resolve-compose-file.sh',
);

function resolveComposeFileFromBash(env: NodeJS.ProcessEnv): string {
  const merged: NodeJS.ProcessEnv = { ...process.env };
  delete merged.COMPOSE_FILE;
  delete merged.OAUTHBEARER_ENABLED;
  delete merged.KAFKA_VERSION;
  Object.assign(merged, env);
  return execFileSync('bash', [resolveComposeFileScript], {
    env: merged,
    encoding: 'utf8',
  }).trim();
}

describe('test/helpers/kafka-version', () => {
  it('defaults KAFKA_VERSION to 4.0', () => {
    expect(getKafkaVersion({})).toBe(DEFAULT_KAFKA_VERSION);
    expect(getKafkaVersion({ KAFKA_VERSION: '  ' })).toBe(DEFAULT_KAFKA_VERSION);
  });

  it('normalizes patch versions to major.minor', () => {
    expect(normalizeKafkaVersion('4.0.0')).toBe('4.0');
    expect(normalizeKafkaVersion('v0.10.2')).toBe('0.10');
    expect(normalizeKafkaVersion('3.6')).toBe('3.6');
  });

  it('compares Kafka versions as semver', () => {
    expect(compareKafkaVersions('4.0', '0.11')).toBeGreaterThan(0);
    expect(compareKafkaVersions('0.10', '0.11')).toBeLessThan(0);
    expect(compareKafkaVersions('2.4.0', '2.4')).toBe(0);
    expect(kafkaVersionAtLeast('0.11', '4.0')).toBe(true);
    expect(kafkaVersionAtMost('0.10', '0.10.2')).toBe(true); // patch is ignored for feature gates
    expect(kafkaVersionAtMost('0.10', '0.11')).toBe(false);
    expect(kafkaVersionEquals('0.11', '0.11.0')).toBe(true);
    expect(kafkaVersionEquals('1.1', '1.1.0')).toBe(true);
    expect(kafkaVersionEquals('0.11', '1.1')).toBe(false);
  });

  it('treats 3.0+ as KRaft in this matrix', () => {
    expect(isKRaftKafkaVersion('2.4')).toBe(false);
    expect(isKRaftKafkaVersion('3.6')).toBe(true);
    expect(isKRaftKafkaVersion('4.0')).toBe(true);
    expect(isKRaftKafkaVersion('4.3')).toBe(true);
  });

  it('resolves the 4.0 KRaft compose file by default', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.0' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft.yml');
  });

  it('resolves the 3.6 KRaft compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '3.6' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-3-6.yml');
  });

  it('resolves the 4.1 KRaft compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.1' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-4-1.yml');
  });

  it('resolves the 4.2 KRaft compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.2' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-4-2.yml');
  });

  it('resolves the 4.3 KRaft compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.3' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-4-3.yml');
  });

  it('resolves the oauthbearer compose file when enabled', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.0', OAUTHBEARER_ENABLED: '1' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-oauthbearer.yml');
  });

  it('honors an explicit COMPOSE_FILE override', () => {
    expect(resolveComposeFile({ COMPOSE_FILE: '/tmp/custom.yml' })).toBe('/tmp/custom.yml');
  });

  it('resolves the 0.11 ZooKeeper compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '0.11' });
    expect(path.basename(composeFile)).toBe('docker-compose.zk-0-11.yml');
    expect(isZooKeeperComposeFile(composeFile)).toBe(true);
  });

  it('resolves the 1.1 ZooKeeper compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '1.1' });
    expect(path.basename(composeFile)).toBe('docker-compose.zk-1-1.yml');
    expect(isZooKeeperComposeFile(composeFile)).toBe(true);
  });

  it('resolves the 2.4 ZooKeeper compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '2.4' });
    expect(path.basename(composeFile)).toBe('docker-compose.zk-2-4.yml');
    expect(isZooKeeperComposeFile(composeFile)).toBe(true);
  });

  it('resolves the 0.10 ZooKeeper compose file', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '0.10' });
    expect(path.basename(composeFile)).toBe('docker-compose.zk-0-10.yml');
    expect(isZooKeeperComposeFile(composeFile)).toBe(true);
  });

  it('does not treat KRaft compose files as ZooKeeper', () => {
    expect(isZooKeeperComposeFile(resolveComposeFile({ KAFKA_VERSION: '4.0' }))).toBe(false);
  });

  it('rejects unknown versions', () => {
    expect(() => resolveComposeFile({ KAFKA_VERSION: '9.9' })).toThrow(/Unsupported KAFKA_VERSION=9.9/);
  });

  it('keeps scripts/resolve-compose-file.sh in sync with the TypeScript mapping', () => {
    for (const version of Object.keys(KAFKA_VERSION_COMPOSE_FILES)) {
      expect(resolveComposeFileFromBash({ KAFKA_VERSION: version })).toBe(
        resolveComposeFile({ KAFKA_VERSION: version }),
      );
    }

    expect(resolveComposeFileFromBash({ OAUTHBEARER_ENABLED: '1' })).toBe(
      resolveComposeFile({ OAUTHBEARER_ENABLED: '1' }),
    );
  });
});
