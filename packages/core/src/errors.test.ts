import { describe, expect, it } from 'vitest';
import {
  isKafkaError,
  isRebalancing,
  KafkaAggregateError,
  KafkaAlterPartitionReassignmentsError,
  KafkaConnectionClosedError,
  KafkaConnectionError,
  KafkaConfigError,
  KafkaConfigRequiresAsyncError,
  KafkaCreateTopicError,
  KafkaDeleteGroupsError,
  KafkaDeleteTopicRecordsError,
  KafkaError,
  KafkaFetcherRebalanceError,
  KafkaInvariantViolation,
  KafkaMemberIdRequired,
  KafkaMetadataNotLoaded,
  KafkaNoBrokerAvailableError,
  KafkaNonRetriableError,
  KafkaNumberOfRetriesExceeded,
  KafkaOffsetOutOfRange,
  KafkaProtocolError,
  KafkaRequestTimeoutError,
  KafkaServerDoesNotSupportApiKey,
  KafkaStaleTopicMetadataAssignment,
  KafkaTopicMetadataNotLoaded,
} from './errors';

describe('errors', () => {
  describe('KafkaError', () => {
    it('defaults to retriable and copies helpUrl from an error-like object', () => {
      const error = new KafkaError({ message: 'boom', helpUrl: 'https://example/help' });
      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe('KafkaError');
      expect(error.message).toBe('boom');
      expect(error.retriable).toBe(true);
      expect(error.helpUrl).toBe('https://example/help');
    });

    it('accepts a string message and a cause', () => {
      const cause = new Error('root');
      const error = new KafkaError('failed', { retriable: false, cause });
      expect(error.message).toBe('failed');
      expect(error.retriable).toBe(false);
      expect(error.helpUrl).toBeUndefined();
      expect(error.cause).toBe(cause);
    });
  });

  describe('KafkaProtocolError', () => {
    const entry = { message: 'not leader', type: 'NOT_LEADER_OR_FOLLOWER', code: 6, retriable: true };

    it('keeps the original message when topic and partition are omitted', () => {
      const error = new KafkaProtocolError(entry);
      expect(error.message).toBe('not leader');
      expect(error.type).toBe('NOT_LEADER_OR_FOLLOWER');
      expect(error.code).toBe(6);
      expect(error.retriable).toBe(true);
    });

    it('appends topic and partition details and lets options override retriable', () => {
      const error = new KafkaProtocolError(entry, { topic: 'orders', partition: 3, retriable: false });
      expect(error.message).toBe('not leader (topic: orders, partition: 3)');
      expect(error.topic).toBe('orders');
      expect(error.partition).toBe(3);
      expect(error.retriable).toBe(false);
    });
  });

  it('attaches topic/partition on KafkaOffsetOutOfRange and memberId on KafkaMemberIdRequired', () => {
    const offsetError = new KafkaOffsetOutOfRange(
      { message: 'oor', type: 'OFFSET_OUT_OF_RANGE', code: 1 },
      {
        topic: 't',
        partition: 2,
      },
    );
    expect(offsetError.topic).toBe('t');
    expect(offsetError.partition).toBe(2);

    const memberError = new KafkaMemberIdRequired(
      { message: 'join', type: 'MEMBER_ID_REQUIRED', code: 79 },
      {
        memberId: 'member-1',
      },
    );
    expect(memberError.memberId).toBe('member-1');
  });

  it('rewrites the stack of KafkaNumberOfRetriesExceeded and records retry metadata', () => {
    const cause = new Error('still failing');
    const error = new KafkaNumberOfRetriesExceeded(cause, { retryCount: 4, retryTime: 1200 });
    expect(error.retriable).toBe(false);
    expect(error.retryCount).toBe(4);
    expect(error.retryTime).toBe(1200);
    expect(error.cause).toBe(cause);
    expect(error.stack).toContain('Caused by:');
    expect(error.stack).toContain('still failing');
  });

  it('builds connection errors with broker identity', () => {
    const closed = new KafkaConnectionClosedError('closed', { host: 'localhost', port: 9092 });
    expect(closed).toBeInstanceOf(KafkaConnectionError);
    expect(closed.broker).toBe('localhost:9092');
    expect(closed.host).toBe('localhost');
    expect(closed.port).toBe(9092);

    const timeout = new KafkaRequestTimeoutError('slow', {
      broker: 'b1',
      correlationId: 9,
      createdAt: 1,
      sentAt: 2,
      pendingDuration: 3,
    });
    expect(timeout.correlationId).toBe(9);
    expect(timeout.pendingDuration).toBe(3);
  });

  it('carries topic metadata on the metadata-not-loaded hierarchy', () => {
    const missing = new KafkaTopicMetadataNotLoaded('missing', { topic: 'orders' });
    expect(missing).toBeInstanceOf(KafkaMetadataNotLoaded);
    expect(missing.topic).toBe('orders');

    const stale = new KafkaStaleTopicMetadataAssignment('stale', { topic: 'orders', unknownPartitions: [9] });
    expect(stale.unknownPartitions).toEqual([9]);
  });

  it('stores delete-groups results and unsupported-api details', () => {
    const groups = new KafkaDeleteGroupsError('failed', [{ groupId: 'g1', errorCode: 69 }]);
    expect(groups.groups).toEqual([{ groupId: 'g1', errorCode: 69 }]);

    const unsupported = new KafkaServerDoesNotSupportApiKey('nope', {
      apiKey: 3,
      apiName: 'Metadata',
      brokerMinVersion: 0,
      brokerMaxVersion: 1,
      implementedVersions: [12],
    });
    expect(unsupported.apiName).toBe('Metadata');
    expect(unsupported.implementedVersions).toEqual([12]);
    expect(unsupported.retriable).toBe(false);
  });

  describe('KafkaDeleteTopicRecordsError', () => {
    it('is retriable only when every present partition error is retriable', () => {
      const retriable = new KafkaDeleteTopicRecordsError({
        partitions: [
          { partition: 0, error: { retriable: true } },
          { partition: 1, error: { retriable: true } },
        ],
      });
      expect(retriable.retriable).toBe(true);

      const mixed = new KafkaDeleteTopicRecordsError({
        partitions: [
          { partition: 0, error: { retriable: true } },
          { partition: 1, error: { retriable: false } },
        ],
      });
      expect(mixed.retriable).toBe(false);
    });

    it('treats partitions without an error as not affecting retriability', () => {
      const error = new KafkaDeleteTopicRecordsError({
        partitions: [{ partition: 0 }, { partition: 1, error: { retriable: true } }],
      });
      expect(error.retriable).toBe(true);
    });
  });

  it('prefixes KafkaInvariantViolation and specializes create/alter protocol errors', () => {
    const invariant = new KafkaInvariantViolation('queue empty');
    expect(invariant.message).toContain('Invariant violated: queue empty');
    expect(invariant.retriable).toBe(false);

    const create = new KafkaCreateTopicError({ message: 'exists', type: 'TOPIC_ALREADY_EXISTS', code: 36 }, 'orders');
    expect(create.topic).toBe('orders');

    const alter = new KafkaAlterPartitionReassignmentsError(
      { message: 'invalid', type: 'INVALID_REPLICA_ASSIGNMENT', code: 38 },
      'orders',
      2,
    );
    expect(alter.topic).toBe('orders');
    expect(alter.partition).toBe(2);
  });

  it('gives KafkaNoBrokerAvailableError a fixed message', () => {
    expect(new KafkaNoBrokerAvailableError().message).toBe('No broker available');
  });

  describe('KafkaConfigError', () => {
    it('carries a tag, an optional path, and is non-retriable', () => {
      const cause = new Error('root cause');
      const error = new KafkaConfigError('MissingBrokers', 'no brokers found', { path: '/x/kafka.config.ts', cause });

      expect(error).toBeInstanceOf(KafkaNonRetriableError);
      expect(error.name).toBe('KafkaConfigError');
      expect(error.tag).toBe('MissingBrokers');
      expect(error.path).toBe('/x/kafka.config.ts');
      expect(error.retriable).toBe(false);
      expect(error.cause).toBe(cause);
    });

    it('leaves path undefined when none is given', () => {
      expect(new KafkaConfigError('ConfigFileNotFound', 'not found').path).toBeUndefined();
    });
  });

  describe('KafkaConfigRequiresAsyncError', () => {
    it('names the path and points at Kafka.fromConfig', () => {
      const error = new KafkaConfigRequiresAsyncError('/x/kafka.config.ts');

      expect(error).toBeInstanceOf(KafkaNonRetriableError);
      expect(error.name).toBe('KafkaConfigRequiresAsyncError');
      expect(error.path).toBe('/x/kafka.config.ts');
      expect(error.message).toContain('/x/kafka.config.ts');
      expect(error.message).toContain('Kafka.fromConfig()');
    });
  });

  describe('isRebalancing', () => {
    it('is true for the three group-rebalance protocol types', () => {
      expect(isRebalancing({ type: 'REBALANCE_IN_PROGRESS' })).toBe(true);
      expect(isRebalancing({ type: 'NOT_COORDINATOR_FOR_GROUP' })).toBe(true);
      expect(isRebalancing({ type: 'ILLEGAL_GENERATION' })).toBe(true);
    });

    it('is false for any other type or a missing type', () => {
      expect(isRebalancing({ type: 'UNKNOWN_MEMBER_ID' })).toBe(false);
      expect(isRebalancing({})).toBe(false);
    });
  });

  describe('isKafkaError', () => {
    it('is true only for the KafkaError hierarchy, not sibling Error subclasses', () => {
      expect(isKafkaError(new KafkaError('x'))).toBe(true);
      expect(isKafkaError(new KafkaNonRetriableError('x'))).toBe(true);
      expect(isKafkaError(new KafkaConfigError('MissingBrokers', 'x'))).toBe(true);
      expect(isKafkaError(new Error('x'))).toBe(false);
      expect(isKafkaError(new KafkaAggregateError('x', []))).toBe(false);
      expect(isKafkaError(new KafkaFetcherRebalanceError())).toBe(false);
      expect(isKafkaError('x')).toBe(false);
    });
  });
});
