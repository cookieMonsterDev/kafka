import { describe, expect, it } from 'vitest';
import { createErrorFromCode, ERROR_CODES, failIfVersionNotSupported, failure, staleMetadata } from './error-codes';

describe('protocol/error-codes', () => {
  it('has a unique code per entry', () => {
    const codes = ERROR_CODES.map((e) => e.code);
    expect(new Set(codes).size).toBe(codes.length);
  });

  describe('failure', () => {
    it('is false for the success code (0)', () => {
      expect(failure(0)).toBe(false);
    });

    it('is true for any non-zero code', () => {
      expect(failure(1)).toBe(true);
      expect(failure(-1)).toBe(true);
    });
  });

  describe('createErrorFromCode', () => {
    it('builds a KafkaProtocolError from a known code', () => {
      const error = createErrorFromCode(1);
      expect(error.type).toBe('OFFSET_OUT_OF_RANGE');
      expect(error.code).toBe(1);
      expect(error.retriable).toBe(false);
      expect(error.message).toContain('offset');
    });

    it('treats THROTTLING_QUOTA_EXCEEDED as retriable', () => {
      const error = createErrorFromCode(89);
      expect(error.type).toBe('THROTTLING_QUOTA_EXCEEDED');
      expect(error.retriable).toBe(true);
    });

    it('uses the Kafka 2.6+ name for code 6', () => {
      const error = createErrorFromCode(6);
      expect(error.type).toBe('NOT_LEADER_OR_FOLLOWER');
      expect(error.retriable).toBe(true);
    });

    it('includes official error codes 90-133', () => {
      for (let code = 90; code <= 133; code++) {
        expect(ERROR_CODES.some((entry) => entry.code === code)).toBe(true);
      }
    });

    it('treats UNKNOWN_TOPIC_ID as retriable', () => {
      const error = createErrorFromCode(100);
      expect(error.type).toBe('UNKNOWN_TOPIC_ID');
      expect(error.retriable).toBe(true);
    });

    it('treats TRANSACTION_ABORTABLE as non-retriable', () => {
      const error = createErrorFromCode(120);
      expect(error.type).toBe('TRANSACTION_ABORTABLE');
      expect(error.retriable).toBe(false);
    });

    it('maps REBOOTSTRAP_REQUIRED', () => {
      const error = createErrorFromCode(129);
      expect(error.type).toBe('REBOOTSTRAP_REQUIRED');
      expect(error.retriable).toBe(false);
    });

    it('treats SHARE_SESSION_LIMIT_REACHED as retriable', () => {
      const error = createErrorFromCode(133);
      expect(error.type).toBe('SHARE_SESSION_LIMIT_REACHED');
      expect(error.retriable).toBe(true);
    });

    it('falls back to a KAFKA_UNKNOWN_ERROR_CODE placeholder for unknown codes', () => {
      const error = createErrorFromCode(9999);
      expect(error.type).toBe('KAFKA_UNKNOWN_ERROR_CODE');
      expect(error.code).toBe(-99);
      expect(error.retriable).toBe(false);
      expect(error.message).toContain('9999');
    });

    it('attaches topic and partition extras to the protocol error', () => {
      const error = createErrorFromCode(1, { topic: 'orders', partition: 3 });
      expect(error.topic).toBe('orders');
      expect(error.partition).toBe(3);
      expect(error.message).toContain('topic: orders');
      expect(error.message).toContain('partition: 3');
    });
  });

  describe('failIfVersionNotSupported', () => {
    it('throws for the unsupported-version code', () => {
      expect(() => failIfVersionNotSupported(35)).toThrow('The version of API is not supported');
    });

    it('does not throw for any other code', () => {
      expect(() => failIfVersionNotSupported(0)).not.toThrow();
      expect(() => failIfVersionNotSupported(1)).not.toThrow();
    });
  });

  describe('staleMetadata', () => {
    it('is true for the classic stale-metadata error types', () => {
      expect(staleMetadata({ type: 'UNKNOWN_TOPIC_OR_PARTITION' })).toBe(true);
      expect(staleMetadata({ type: 'LEADER_NOT_AVAILABLE' })).toBe(true);
      expect(staleMetadata({ type: 'NOT_LEADER_OR_FOLLOWER' })).toBe(true);
    });

    it('is true for the pre-2.6 NOT_LEADER_FOR_PARTITION alias', () => {
      expect(staleMetadata({ type: 'NOT_LEADER_FOR_PARTITION' })).toBe(true);
    });

    it('is true for topic-id, leader-epoch, and rebootstrap errors', () => {
      expect(staleMetadata({ type: 'UNKNOWN_TOPIC_ID' })).toBe(true);
      expect(staleMetadata({ type: 'INCONSISTENT_TOPIC_ID' })).toBe(true);
      expect(staleMetadata({ type: 'FENCED_LEADER_EPOCH' })).toBe(true);
      expect(staleMetadata({ type: 'UNKNOWN_LEADER_EPOCH' })).toBe(true);
      expect(staleMetadata({ type: 'REBOOTSTRAP_REQUIRED' })).toBe(true);
    });

    it('is false for any other error type', () => {
      expect(staleMetadata({ type: 'OFFSET_OUT_OF_RANGE' })).toBe(false);
      expect(staleMetadata({})).toBe(false);
    });
  });
});
