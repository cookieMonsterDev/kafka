import { describe, expect, it } from 'vitest';
import { isSecurityDisabledError, isUnknownTopicOrPartitionError, protocolErrorType } from './protocol-error';

describe('protocolErrorType', () => {
  it('reads the type directly off the error', () => {
    expect(protocolErrorType({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('walks .cause to find the type on a retry-exhausted wrapper', () => {
    const error = { type: undefined, cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' } };
    expect(protocolErrorType(error)).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('walks .errors to find the type on an aggregate error', () => {
    const error = { name: 'KafkaAggregateError', errors: [{ type: 'UNKNOWN_TOPIC_OR_PARTITION' }] };
    expect(protocolErrorType(error)).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('finds the type in a later entry of .errors, not just the first', () => {
    const error = { errors: [{ type: undefined }, { type: 'UNKNOWN_TOPIC_OR_PARTITION' }] };
    expect(protocolErrorType(error)).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('returns undefined when neither .cause nor .errors carries a type', () => {
    expect(protocolErrorType({ message: 'boom' })).toBeUndefined();
    expect(protocolErrorType(null)).toBeUndefined();
    expect(protocolErrorType(undefined)).toBeUndefined();
  });
});

describe('isUnknownTopicOrPartitionError', () => {
  it('recognizes the type via either shape', () => {
    expect(isUnknownTopicOrPartitionError({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(true);
    expect(isUnknownTopicOrPartitionError({ errors: [{ type: 'UNKNOWN_TOPIC_OR_PARTITION' }] })).toBe(true);
    expect(isUnknownTopicOrPartitionError({ type: 'TOPIC_ALREADY_EXISTS' })).toBe(false);
  });
});

describe('isSecurityDisabledError', () => {
  it('recognizes the type', () => {
    expect(isSecurityDisabledError({ type: 'SECURITY_DISABLED' })).toBe(true);
    expect(isSecurityDisabledError({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(false);
  });
});
