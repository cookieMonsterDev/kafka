import { describe, expect, it } from 'vitest';
import {
  KafkaAggregateError,
  KafkaBrokerNotFound,
  KafkaConnectionClosedError,
  KafkaConnectionError,
  KafkaDeleteGroupsError,
  KafkaDeleteTopicRecordsError,
  KafkaMetadataNotLoaded,
} from '../errors';
import { createErrorFromCode, ERROR_CODES } from '../protocol/error-codes';
import {
  closedConnectionAddress,
  COORDINATOR_LOADING_ERRORS,
  hasTargetMoved,
  isGroupIdNotFound,
  isRebalancing,
  isRetriableFor,
  isStaleMetadata,
  RECOVERY_ACTIONS,
  recoveryAction,
  STALE_METADATA_ERRORS,
  unionOf,
} from './error-classifier';

function errorOfType(type: string) {
  return createErrorFromCode(ERROR_CODES.find((entry) => entry.type === type)!.code);
}

describe('cluster/error-classifier', () => {
  describe('isStaleMetadata', () => {
    it.each([
      'UNKNOWN_TOPIC_OR_PARTITION',
      'LEADER_NOT_AVAILABLE',
      'NOT_LEADER_OR_FOLLOWER',
      'NOT_LEADER_FOR_PARTITION',
      'UNKNOWN_TOPIC_ID',
      'INCONSISTENT_TOPIC_ID',
      'FENCED_LEADER_EPOCH',
      'UNKNOWN_LEADER_EPOCH',
      'REBOOTSTRAP_REQUIRED',
    ])('is true for %s', (type) => {
      expect(isStaleMetadata({ type })).toBe(true);
    });

    it('is false for any other error type, a missing type or no error', () => {
      expect(isStaleMetadata({ type: 'OFFSET_OUT_OF_RANGE' })).toBe(false);
      expect(isStaleMetadata({})).toBe(false);
      expect(isStaleMetadata(undefined)).toBe(false);
    });
  });

  describe('isGroupIdNotFound', () => {
    it('is true only for GROUP_ID_NOT_FOUND', () => {
      expect(isGroupIdNotFound(errorOfType('GROUP_ID_NOT_FOUND'))).toBe(true);
      expect(isGroupIdNotFound({ type: 'NON_EMPTY_GROUP' })).toBe(false);
      expect(isGroupIdNotFound(undefined)).toBe(false);
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

  describe('closedConnectionAddress', () => {
    it('returns the address of a closed connection', () => {
      const error = new KafkaConnectionClosedError('closed', { host: 'broker-1', port: 9092 });
      expect(closedConnectionAddress(error)).toEqual({ host: 'broker-1', port: 9092 });
    });

    it('returns null for a closed connection without an address or another error', () => {
      expect(closedConnectionAddress(new KafkaConnectionClosedError('closed'))).toBeNull();
      expect(closedConnectionAddress(new KafkaConnectionError('refused'))).toBeNull();
      expect(closedConnectionAddress(new Error('boom'))).toBeNull();
    });
  });

  describe('recoveryAction', () => {
    it('refreshes metadata after stale metadata, NOT_CONTROLLER, an unavailable replica or missing metadata', () => {
      expect(recoveryAction(errorOfType('NOT_LEADER_OR_FOLLOWER'))).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
      expect(recoveryAction(errorOfType('NOT_CONTROLLER'))).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
      expect(recoveryAction(errorOfType('REPLICA_NOT_AVAILABLE'))).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
      expect(recoveryAction(new KafkaBrokerNotFound('gone'))).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
      expect(recoveryAction(new KafkaMetadataNotLoaded('none'))).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
    });

    it('re-finds the coordinator after a moved or unavailable coordinator', () => {
      expect(recoveryAction(errorOfType('NOT_COORDINATOR_FOR_GROUP'))).toBe(RECOVERY_ACTIONS.FIND_COORDINATOR);
      expect(recoveryAction(errorOfType('GROUP_COORDINATOR_NOT_AVAILABLE'))).toBe(RECOVERY_ACTIONS.FIND_COORDINATOR);
    });

    it('drops the broker of a closed connection and reconnects after any other connection error', () => {
      const closed = new KafkaConnectionClosedError('closed', { host: 'broker-1', port: 9092 });
      expect(recoveryAction(closed)).toBe(RECOVERY_ACTIONS.DROP_BROKER);
      expect(recoveryAction(new KafkaConnectionClosedError('closed'))).toBe(RECOVERY_ACTIONS.RECONNECT);
      expect(recoveryAction(new KafkaConnectionError('refused', { code: 'ECONNREFUSED' }))).toBe(
        RECOVERY_ACTIONS.RECONNECT,
      );
    });

    it('gives up on any other error', () => {
      expect(recoveryAction(errorOfType('GROUP_LOAD_IN_PROGRESS'))).toBe(RECOVERY_ACTIONS.GIVE_UP);
      expect(recoveryAction(new Error('boom'))).toBe(RECOVERY_ACTIONS.GIVE_UP);
    });

    it('takes the first recovery one of an aggregate error needs', () => {
      const aggregate = new KafkaAggregateError('errors', [
        errorOfType('TOPIC_ALREADY_EXISTS'),
        errorOfType('LEADER_NOT_AVAILABLE'),
      ]);
      expect(recoveryAction(aggregate)).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);

      const records = new KafkaDeleteTopicRecordsError({
        partitions: [{ partition: 0, offset: '1', error: new KafkaMetadataNotLoaded('Topic metadata not loaded') }],
      });
      expect(recoveryAction(records)).toBe(RECOVERY_ACTIONS.REFRESH_METADATA);
    });
  });

  describe('hasTargetMoved', () => {
    it('is NOT_CONTROLLER for the controller', () => {
      expect(hasTargetMoved(errorOfType('NOT_CONTROLLER'), 'controller')).toBe(true);
      expect(hasTargetMoved(errorOfType('NOT_LEADER_OR_FOLLOWER'), 'controller')).toBe(false);
    });

    it('is a moved, unavailable or unreachable coordinator for a coordinator', () => {
      expect(hasTargetMoved(errorOfType('NOT_COORDINATOR_FOR_GROUP'), 'coordinator')).toBe(true);
      expect(hasTargetMoved(errorOfType('GROUP_COORDINATOR_NOT_AVAILABLE'), 'coordinator')).toBe(true);
      expect(hasTargetMoved(new KafkaConnectionError('refused', { code: 'ECONNREFUSED' }), 'coordinator')).toBe(true);
      expect(hasTargetMoved(errorOfType('GROUP_LOAD_IN_PROGRESS'), 'coordinator')).toBe(false);
    });

    it('is stale metadata for a partition leader', () => {
      expect(hasTargetMoved(errorOfType('NOT_LEADER_OR_FOLLOWER'), 'leader')).toBe(true);
      expect(hasTargetMoved(errorOfType('NOT_CONTROLLER'), 'leader')).toBe(false);
    });

    it('is never true for a node or any broker', () => {
      expect(hasTargetMoved(errorOfType('NOT_LEADER_OR_FOLLOWER'), 'node')).toBe(false);
      expect(hasTargetMoved(errorOfType('NOT_CONTROLLER'), 'any')).toBe(false);
    });
  });

  describe('isRetriableFor', () => {
    it('retries a moved target or an error type in retryOn', () => {
      expect(isRetriableFor(errorOfType('NOT_CONTROLLER'), ['controller'], undefined)).toBe(true);
      expect(isRetriableFor(errorOfType('GROUP_LOAD_IN_PROGRESS'), ['coordinator'], undefined)).toBe(false);
      expect(isRetriableFor(errorOfType('GROUP_LOAD_IN_PROGRESS'), ['coordinator'], COORDINATOR_LOADING_ERRORS)).toBe(
        true,
      );
    });

    it('retries for any of several target kinds', () => {
      expect(isRetriableFor(errorOfType('NOT_CONTROLLER'), ['node', 'controller'], undefined)).toBe(true);
      expect(isRetriableFor(errorOfType('NOT_CONTROLLER'), [], undefined)).toBe(false);
    });

    it('retries a KafkaDeleteGroupsError only when every group error is retriable', () => {
      const retryOn = COORDINATOR_LOADING_ERRORS;
      const loading = new KafkaDeleteGroupsError('delete', [
        { groupId: 'a', errorCode: 14, error: errorOfType('GROUP_LOAD_IN_PROGRESS') },
        { groupId: 'b', errorCode: 16, error: errorOfType('NOT_COORDINATOR_FOR_GROUP') },
      ]);
      expect(isRetriableFor(loading, ['coordinator'], retryOn)).toBe(true);

      const mixed = new KafkaDeleteGroupsError('delete', [
        { groupId: 'a', errorCode: 14, error: errorOfType('GROUP_LOAD_IN_PROGRESS') },
        { groupId: 'b', errorCode: 69, error: errorOfType('GROUP_ID_NOT_FOUND') },
      ]);
      expect(isRetriableFor(mixed, ['coordinator'], retryOn)).toBe(false);
      expect(isRetriableFor(new KafkaDeleteGroupsError('none', []), ['coordinator'], retryOn)).toBe(false);
    });

    it('does not retry any other aggregate error, even when all of its errors are retriable', () => {
      const aggregate = new KafkaAggregateError('errors', [errorOfType('NOT_CONTROLLER')]);
      expect(isRetriableFor(aggregate, ['controller'], undefined)).toBe(false);
    });
  });

  describe('unionOf', () => {
    it('holds every error type of the given sets', () => {
      const union = unionOf(COORDINATOR_LOADING_ERRORS, STALE_METADATA_ERRORS);
      expect(union.has('GROUP_LOAD_IN_PROGRESS')).toBe(true);
      expect(union.has('NOT_LEADER_OR_FOLLOWER')).toBe(true);
      expect(union.size).toBe(COORDINATOR_LOADING_ERRORS.size + STALE_METADATA_ERRORS.size);
    });
  });
});
