import { describe, expect, it } from 'vitest';
import { ADMIN_METHOD_CLASSIFICATION, READ_ONLY_ADMIN_METHODS } from './method-classification';

describe('ADMIN_METHOD_CLASSIFICATION', () => {
  it('classifies every method as mounted or passthrough-only, never anything else', () => {
    for (const classification of Object.values(ADMIN_METHOD_CLASSIFICATION)) {
      expect(['mounted', 'passthrough-only']).toContain(classification);
    }
  });

  it('has no duplicate-looking gaps — at least the known mounted methods are present', () => {
    expect(ADMIN_METHOD_CLASSIFICATION.listTopics).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.createTopics).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.describeTopicPartitions).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.fetchTopicMetadata).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.describeCluster).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.describeConfigs).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.incrementalAlterConfigs).toBe('mounted');
    expect(ADMIN_METHOD_CLASSIFICATION.listConfigResources).toBe('mounted');
  });
});

describe('READ_ONLY_ADMIN_METHODS', () => {
  it('is a subset of the classified method names', () => {
    const known = new Set(Object.keys(ADMIN_METHOD_CLASSIFICATION));
    for (const method of READ_ONLY_ADMIN_METHODS) {
      expect(known.has(method)).toBe(true);
    }
  });

  it('does not include an obviously destructive method', () => {
    expect(READ_ONLY_ADMIN_METHODS.has('deleteTopics')).toBe(false);
    expect(READ_ONLY_ADMIN_METHODS.has('createTopics')).toBe(false);
  });
});
