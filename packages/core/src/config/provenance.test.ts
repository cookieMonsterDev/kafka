import { describe, expect, it } from 'vitest';
import { buildKafkaConfigSource } from './provenance';

describe('buildKafkaConfigSource', () => {
  it('tags every key explicit, file, or default, and carries no values', () => {
    const source = buildKafkaConfigSource(
      { brokers: ['explicit:9092'], clientId: 'explicit-client' },
      { client: { clientId: 'file-client', requestTimeout: 999 } },
      '/path/to/kafka.config.ts',
    );

    expect(source.path).toBe('/path/to/kafka.config.ts');
    expect(source.keys.brokers).toBe('explicit');
    expect(source.keys.clientId).toBe('explicit');
    expect(source.keys.requestTimeout).toBe('file');
    expect(source.keys.connectionTimeout).toBe('default');
    expect(source).not.toHaveProperty('config');
    for (const value of Object.values(source.keys)) {
      expect(typeof value).toBe('string');
    }
  });

  it('reports every key default and a null path when no file was used', () => {
    const source = buildKafkaConfigSource({ brokers: ['explicit:9092'] }, null, null);

    expect(source.path).toBeNull();
    expect(source.keys.brokers).toBe('explicit');
    expect(source.keys.clientId).toBe('default');
    expect(source.keys.retry).toBe('default');
  });

  it('classifies every KafkaConfig key (21 keys)', () => {
    const source = buildKafkaConfigSource({}, null, null);

    expect(Object.keys(source.keys)).toHaveLength(21);
  });

  it('an explicit undefined value does not shadow a file-provided value', () => {
    const source = buildKafkaConfigSource(
      { clientId: undefined },
      { client: { clientId: 'file-client' } },
      '/path/to/kafka.config.ts',
    );

    expect(source.keys.clientId).toBe('file');
  });
});
