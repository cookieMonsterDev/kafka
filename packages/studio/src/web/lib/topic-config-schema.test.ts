import { describe, expect, it } from 'vitest';
import { validateTopicConfigValue } from './topic-config-schema';

describe('validateTopicConfigValue', () => {
  it('accepts a known-good enum value', () => {
    expect(validateTopicConfigValue('cleanup.policy', 'compact')).toBeNull();
  });

  it('rejects an unknown enum value', () => {
    expect(validateTopicConfigValue('cleanup.policy', 'shred')).toContain('must be one of');
  });

  it('accepts "true"/"false" for a boolean config', () => {
    expect(validateTopicConfigValue('preallocate', 'true')).toBeNull();
    expect(validateTopicConfigValue('preallocate', 'false')).toBeNull();
  });

  it('rejects a non-boolean value for a boolean config', () => {
    expect(validateTopicConfigValue('preallocate', 'yes')).not.toBeNull();
  });

  it('accepts a non-negative integer for a duration config', () => {
    expect(validateTopicConfigValue('retention.ms', '604800000')).toBeNull();
  });

  it('rejects a negative value for a duration config that does not allow -1', () => {
    expect(validateTopicConfigValue('segment.ms', '-1')).not.toBeNull();
  });

  it('accepts -1 for a config that allows "unlimited"', () => {
    expect(validateTopicConfigValue('retention.ms', '-1')).toBeNull();
  });

  it('rejects a non-numeric value for a duration config', () => {
    expect(validateTopicConfigValue('retention.ms', 'forever')).not.toBeNull();
  });

  it('accepts a ratio between 0 and 1', () => {
    expect(validateTopicConfigValue('min.cleanable.dirty.ratio', '0.5')).toBeNull();
  });

  it('rejects a ratio outside 0 to 1', () => {
    expect(validateTopicConfigValue('min.cleanable.dirty.ratio', '1.5')).not.toBeNull();
  });

  it('rejects an empty value for a known key', () => {
    expect(validateTopicConfigValue('retention.ms', '   ')).toBe('value is required');
  });

  it('has no opinion on a key it does not know', () => {
    expect(validateTopicConfigValue('some.custom.plugin.setting', 'anything')).toBeNull();
  });
});
