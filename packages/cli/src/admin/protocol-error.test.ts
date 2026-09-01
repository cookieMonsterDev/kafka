import { describe, expect, it } from 'vitest';
import { isUnknownTopicOrPartitionError, protocolErrorType } from './protocol-error';

describe('protocolErrorType', () => {
  it('reads .type directly off the error', () => {
    expect(protocolErrorType({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('walks .cause when the error itself has no .type', () => {
    const error = { cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' } };
    expect(protocolErrorType(error)).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('walks multiple levels of .cause', () => {
    const error = { cause: { cause: { type: 'NOT_CONTROLLER' } } };
    expect(protocolErrorType(error)).toBe('NOT_CONTROLLER');
  });

  it('returns undefined when nothing in the chain has a .type', () => {
    expect(protocolErrorType(new Error('boom'))).toBeUndefined();
    expect(protocolErrorType(null)).toBeUndefined();
    expect(protocolErrorType('boom')).toBeUndefined();
  });

  it('does not loop forever on a cyclical .cause chain', () => {
    const error: { type?: string; cause?: unknown } = {};
    error.cause = error;
    expect(protocolErrorType(error)).toBeUndefined();
  });
});

describe('isUnknownTopicOrPartitionError', () => {
  it('is true for a direct UNKNOWN_TOPIC_OR_PARTITION error', () => {
    expect(isUnknownTopicOrPartitionError({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(true);
  });

  it('is true when wrapped by a retry-exhaustion error', () => {
    const wrapped = { name: 'KafkaNumberOfRetriesExceeded', cause: { type: 'UNKNOWN_TOPIC_OR_PARTITION' } };
    expect(isUnknownTopicOrPartitionError(wrapped)).toBe(true);
  });

  it('is false for an unrelated error', () => {
    expect(isUnknownTopicOrPartitionError(new Error('boom'))).toBe(false);
    expect(isUnknownTopicOrPartitionError({ type: 'NOT_CONTROLLER' })).toBe(false);
  });
});
