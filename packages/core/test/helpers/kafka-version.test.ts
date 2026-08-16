import path from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  compareKafkaVersions,
  DEFAULT_KAFKA_VERSION,
  getKafkaVersion,
  isKRaftKafkaVersion,
  kafkaVersionAtLeast,
  kafkaVersionAtMost,
  normalizeKafkaVersion,
  resolveComposeFile,
} from './kafka-version';

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
  });

  it('treats 3.0+ as KRaft in this matrix', () => {
    expect(isKRaftKafkaVersion('2.4')).toBe(false);
    expect(isKRaftKafkaVersion('3.6')).toBe(true);
    expect(isKRaftKafkaVersion('4.0')).toBe(true);
  });

  it('resolves the 4.0 KRaft compose file by default', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.0' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft.yml');
  });

  it('resolves the oauthbearer compose file when enabled', () => {
    const composeFile = resolveComposeFile({ KAFKA_VERSION: '4.0', OAUTHBEARER_ENABLED: '1' });
    expect(path.basename(composeFile)).toBe('docker-compose.kraft-oauthbearer.yml');
  });

  it('honors an explicit COMPOSE_FILE override', () => {
    expect(resolveComposeFile({ COMPOSE_FILE: '/tmp/custom.yml' })).toBe('/tmp/custom.yml');
  });

  it('rejects versions whose compose file is not in test/assets yet', () => {
    expect(() => resolveComposeFile({ KAFKA_VERSION: '2.4' })).toThrow(/not in test\/assets yet/);
  });

  it('rejects unknown versions', () => {
    expect(() => resolveComposeFile({ KAFKA_VERSION: '9.9' })).toThrow(/Unsupported KAFKA_VERSION=9.9/);
  });
});
