/**
 * Sorts broker errors into the recovery they need. Every retry and recovery decision that depends
 * on a protocol error type reads it here, so a new recovery rule lands in one place. Each check is
 * a `Set` lookup on `error.type` that allocates nothing, so produce and fetch use it on their
 * error paths too.
 */

import type { BrokerTarget } from './run';

/** What the client does about an error before it tries the request again. */
export const RECOVERY_ACTIONS = {
  /** The client's metadata is stale: refresh it, then resolve the target again. */
  REFRESH_METADATA: 'refresh-metadata',
  /** The coordinator moved or is not available yet: look it up again. */
  FIND_COORDINATOR: 'find-coordinator',
  /** The connection to one broker closed: drop that broker so the next lookup reconnects it. */
  DROP_BROKER: 'drop-broker',
  /** The client could not reach a broker: reconnect the cluster if needed and refresh metadata. */
  RECONNECT: 'reconnect',
  /** No recovery is known. */
  GIVE_UP: 'give-up',
} as const;

export type RecoveryAction = (typeof RECOVERY_ACTIONS)[keyof typeof RECOVERY_ACTIONS];

/** The kind of broker a request goes to. */
export type TargetKind = BrokerTarget['kind'];

/** Errors meaning the client's view of topics, partitions or partition leaders is out of date. */
export const STALE_METADATA_ERRORS: ReadonlySet<string> = new Set([
  'UNKNOWN_TOPIC_OR_PARTITION',
  'LEADER_NOT_AVAILABLE',
  'NOT_LEADER_FOR_PARTITION',
  'NOT_LEADER_OR_FOLLOWER',
  'UNKNOWN_TOPIC_ID',
  'INCONSISTENT_TOPIC_ID',
  'FENCED_LEADER_EPOCH',
  'UNKNOWN_LEADER_EPOCH',
  'REBOOTSTRAP_REQUIRED',
]);

/** The request reached a broker that is not the controller. */
export const NOT_CONTROLLER_ERRORS: ReadonlySet<string> = new Set(['NOT_CONTROLLER']);

/** The coordinator of a group or transactional id moved or is not available yet. */
export const STALE_COORDINATOR_ERRORS: ReadonlySet<string> = new Set([
  'NOT_COORDINATOR_FOR_GROUP',
  'GROUP_COORDINATOR_NOT_AVAILABLE',
]);

/** The coordinator is still loading the group's or transactional id's state. */
export const COORDINATOR_LOADING_ERRORS: ReadonlySet<string> = new Set(['GROUP_LOAD_IN_PROGRESS']);

/**
 * The transactional id's previous transaction is still ending. The previous producer may have
 * crashed mid-transaction; retrying lets the coordinator abort it.
 * @see https://github.com/apache/kafka/blob/201da0542726472d954080d54bc585b111aaf86f/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L1001-L1002
 */
export const CONCURRENT_TRANSACTIONS_ERRORS: ReadonlySet<string> = new Set(['CONCURRENT_TRANSACTIONS']);

/** A replica or broker the request needs is down; fresh metadata may route around it. */
export const REPLICA_UNAVAILABLE_ERRORS: ReadonlySet<string> = new Set([
  'REPLICA_NOT_AVAILABLE',
  'BROKER_NOT_AVAILABLE',
]);

const LEADER_EPOCH_ERRORS: ReadonlySet<string> = new Set(['FENCED_LEADER_EPOCH', 'UNKNOWN_LEADER_EPOCH']);
const FETCH_SESSION_ERRORS: ReadonlySet<string> = new Set([
  'FETCH_SESSION_ID_NOT_FOUND',
  'INVALID_FETCH_SESSION_EPOCH',
]);
const UNKNOWN_TOPIC_ERRORS: ReadonlySet<string> = new Set(['UNKNOWN_TOPIC_OR_PARTITION', 'INVALID_TOPIC_EXCEPTION']);
const TOPIC_AUTHORIZATION_ERRORS: ReadonlySet<string> = new Set(['TOPIC_AUTHORIZATION_FAILED']);
const REBALANCE_ERRORS: ReadonlySet<string> = new Set([
  'REBALANCE_IN_PROGRESS',
  'NOT_COORDINATOR_FOR_GROUP',
  'ILLEGAL_GENERATION',
]);
const UNKNOWN_MEMBER_ERRORS: ReadonlySet<string> = new Set(['UNKNOWN_MEMBER_ID']);
const REJECTED_MEMBER_ERRORS: ReadonlySet<string> = new Set(['UNKNOWN_MEMBER_ID', 'FENCED_MEMBER_EPOCH']);
const PRODUCER_FENCED_ERRORS: ReadonlySet<string> = new Set(['PRODUCER_FENCED', 'INVALID_PRODUCER_EPOCH']);
const UNKNOWN_PRODUCER_ID_ERRORS: ReadonlySet<string> = new Set(['UNKNOWN_PRODUCER_ID']);
const TOPIC_EXISTS_ERRORS: ReadonlySet<string> = new Set(['TOPIC_ALREADY_EXISTS']);
/** Errors the broker returns although the records may already be on the leader's log. */
const WRITE_OUTCOME_UNKNOWN_ERRORS: ReadonlySet<string> = new Set([
  'REQUEST_TIMED_OUT',
  'NOT_ENOUGH_REPLICAS_AFTER_APPEND',
  'OUT_OF_ORDER_SEQUENCE_NUMBER',
]);
const UNSUPPORTED_VERSION_ERRORS: ReadonlySet<string> = new Set(['UNSUPPORTED_VERSION']);
const UNKNOWN_SUBSCRIPTION_ERRORS: ReadonlySet<string> = new Set(['UNKNOWN_SUBSCRIPTION_ID']);
const TELEMETRY_TOO_LARGE_ERRORS: ReadonlySet<string> = new Set(['TELEMETRY_TOO_LARGE']);
const REQUEST_TIMED_OUT_ERRORS: ReadonlySet<string> = new Set(['REQUEST_TIMED_OUT']);
const SASL_STATE_ERRORS: ReadonlySet<string> = new Set(['ILLEGAL_SASL_STATE']);
const REBOOTSTRAP_ERRORS: ReadonlySet<string> = new Set(['REBOOTSTRAP_REQUIRED']);

/** Client-side errors that a metadata refresh can fix: a missing broker or missing metadata. */
const STALE_METADATA_ERROR_NAMES: ReadonlySet<string> = new Set([
  'KafkaBrokerNotFound',
  'KafkaMetadataNotLoaded',
  'KafkaTopicMetadataNotLoaded',
]);

const CONNECTION_ERROR_NAMES: ReadonlySet<string> = new Set(['KafkaConnectionError', 'KafkaConnectionClosedError']);

const CONNECTION_REFUSED = 'ECONNREFUSED';

interface ErrorDescriptor {
  type?: string;
  name?: string;
  code?: unknown;
  host?: string;
  port?: number;
}

function hasType(error: unknown, types: ReadonlySet<string>): boolean {
  const type = (error as ErrorDescriptor | null | undefined)?.type;
  return type != null && types.has(type);
}

function nameOf(error: unknown): string | undefined {
  return (error as ErrorDescriptor | null | undefined)?.name;
}

/** The client's view of topics, partitions or partition leaders is out of date. */
export function isStaleMetadata(error: unknown): boolean {
  return hasType(error, STALE_METADATA_ERRORS);
}

/** The coordinator moved or is not available yet. */
export function isStaleCoordinator(error: unknown): boolean {
  return hasType(error, STALE_COORDINATOR_ERRORS);
}

/** A fetch based on an older leader epoch: the partition may have been truncated. */
export function isStaleLeaderEpoch(error: unknown): boolean {
  return hasType(error, LEADER_EPOCH_ERRORS);
}

/** The broker no longer recognizes this fetch session (KIP-227), so the next fetch must be a full one. */
export function isFetchSessionInvalid(error: unknown): boolean {
  return hasType(error, FETCH_SESSION_ERRORS);
}

/** The topic does not exist or its name is invalid. */
export function isUnknownTopic(error: unknown): boolean {
  return hasType(error, UNKNOWN_TOPIC_ERRORS);
}

/** The client may not describe the topic. */
export function isTopicAuthorizationFailed(error: unknown): boolean {
  return hasType(error, TOPIC_AUTHORIZATION_ERRORS);
}

/** The topic to create exists already. */
export function isTopicAlreadyExists(error: unknown): boolean {
  return hasType(error, TOPIC_EXISTS_ERRORS);
}

/** The broker gave up waiting for the request to complete, such as a topic deletion. */
export function isRequestTimedOut(error: unknown): boolean {
  return hasType(error, REQUEST_TIMED_OUT_ERRORS);
}

/** The produce request failed, but its records may still have been appended. */
export function isWriteOutcomeUnknown(error: unknown): boolean {
  return hasType(error, WRITE_OUTCOME_UNKNOWN_ERRORS);
}

/** The broker does not support the request's API version. */
export function isUnsupportedVersion(error: unknown): boolean {
  return hasType(error, UNSUPPORTED_VERSION_ERRORS);
}

/** The broker no longer knows the client's telemetry subscription (KIP-714); it has to subscribe again. */
export function isUnknownTelemetrySubscription(error: unknown): boolean {
  return hasType(error, UNKNOWN_SUBSCRIPTION_ERRORS);
}

/** The pushed telemetry exceeds the broker's limit (KIP-714). */
export function isTelemetryTooLarge(error: unknown): boolean {
  return hasType(error, TELEMETRY_TOO_LARGE_ERRORS);
}

/** The consumer group is rebalancing or its coordinator moved: the member has to rejoin. */
export function isRebalancing(error: unknown): boolean {
  return hasType(error, REBALANCE_ERRORS);
}

/** The coordinator does not know the member id; the member rejoins without it. */
export function isUnknownMember(error: unknown): boolean {
  return hasType(error, UNKNOWN_MEMBER_ERRORS);
}

/** The member is unknown to the coordinator or was fenced by a newer member epoch. */
export function isRejectedMember(error: unknown): boolean {
  return hasType(error, REJECTED_MEMBER_ERRORS);
}

/** A newer instance holds the producer's transactional id. */
export function isProducerFenced(error: unknown): boolean {
  return hasType(error, PRODUCER_FENCED_ERRORS);
}

/** The broker dropped the producer id, for example after its retention ran out. */
export function isUnknownProducerId(error: unknown): boolean {
  return hasType(error, UNKNOWN_PRODUCER_ID_ERRORS);
}

/** The connection's SASL state cannot recover; its connection pool has to be rebuilt. */
export function isIllegalSaslState(error: unknown): boolean {
  return hasType(error, SASL_STATE_ERRORS);
}

/** The broker asks the client to rediscover the cluster from its bootstrap brokers (KIP-1102). */
export function isRebootstrapRequired(error: unknown): boolean {
  return hasType(error, REBOOTSTRAP_ERRORS);
}

/** A connection to a broker failed or closed. */
export function isConnectionError(error: unknown): boolean {
  const name = nameOf(error);
  return name != null && CONNECTION_ERROR_NAMES.has(name);
}

/** The broker refused the connection, for example because it is down for maintenance. */
export function isConnectionRefused(error: unknown): boolean {
  return (error as ErrorDescriptor | null | undefined)?.code === CONNECTION_REFUSED;
}

/** The closed connection's broker address, when the error names one. */
export function closedConnectionAddress(error: unknown): { host: string; port: number } | null {
  if (nameOf(error) !== 'KafkaConnectionClosedError') return null;
  const { host, port } = error as ErrorDescriptor;
  return host != null && port != null ? { host, port } : null;
}

/**
 * The errors an aggregate error carries, one per topic, partition or group, or `null` for an error
 * that is not an aggregate.
 */
function childErrors(error: unknown): readonly unknown[] | null {
  switch (nameOf(error)) {
    case 'KafkaAggregateError':
      return (error as { errors: readonly unknown[] }).errors;
    case 'KafkaDeleteGroupsError':
      return (error as { groups: readonly { error?: unknown }[] }).groups.map(({ error: child }) => child);
    case 'KafkaDeleteTopicRecordsError':
      return (error as { partitions: readonly { error?: unknown }[] }).partitions.map(({ error: child }) => child);
    default:
      return null;
  }
}

/**
 * The recovery an error needs before another attempt. An aggregate error takes the first recovery
 * one of its errors needs.
 */
export function recoveryAction(error: unknown): RecoveryAction {
  const children = childErrors(error);
  if (children) {
    for (const child of children) {
      const action = recoveryAction(child);
      if (action !== RECOVERY_ACTIONS.GIVE_UP) return action;
    }
    return RECOVERY_ACTIONS.GIVE_UP;
  }

  if (closedConnectionAddress(error)) return RECOVERY_ACTIONS.DROP_BROKER;
  if (isConnectionError(error)) return RECOVERY_ACTIONS.RECONNECT;
  if (
    isStaleMetadata(error) ||
    hasType(error, NOT_CONTROLLER_ERRORS) ||
    hasType(error, REPLICA_UNAVAILABLE_ERRORS) ||
    STALE_METADATA_ERROR_NAMES.has(nameOf(error) ?? '')
  ) {
    return RECOVERY_ACTIONS.REFRESH_METADATA;
  }
  if (isStaleCoordinator(error)) return RECOVERY_ACTIONS.FIND_COORDINATOR;
  return RECOVERY_ACTIONS.GIVE_UP;
}

/**
 * Whether the error means the request went to a broker that no longer holds the target's role, so
 * resolving the target again can succeed: a controller that lost its role, a coordinator that
 * moved or refused the connection, or a partition leader that changed.
 */
export function hasTargetMoved(error: unknown, kind: TargetKind): boolean {
  switch (kind) {
    case 'controller':
      return hasType(error, NOT_CONTROLLER_ERRORS);
    case 'coordinator':
      // During maintenance the coordinator can go down; the next lookup finds its successor.
      return isStaleCoordinator(error) || isConnectionRefused(error);
    case 'leader':
      return isStaleMetadata(error);
    case 'node':
    case 'any':
      return false;
  }
}

/**
 * Whether another attempt at `kinds` can succeed after the error: the target moved, or the error
 * type is one of `retryOn`. A `KafkaDeleteGroupsError` is retriable only when every group's error
 * is, so the remaining groups can be deleted again; any other aggregate error is not retried.
 */
export function isRetriableFor(
  error: unknown,
  kinds: readonly TargetKind[],
  retryOn: ReadonlySet<string> | undefined,
): boolean {
  if (nameOf(error) === 'KafkaDeleteGroupsError') {
    const groups = childErrors(error) ?? [];
    return groups.length > 0 && groups.every((group) => isRetriableFor(group, kinds, retryOn));
  }
  if (retryOn && hasType(error, retryOn)) return true;
  return kinds.some((kind) => hasTargetMoved(error, kind));
}

/** One set holding every error type in `sets`, built once where a caller defines its `retryOn`. */
export function unionOf(...sets: readonly ReadonlySet<string>[]): ReadonlySet<string> {
  return new Set(sets.flatMap((set) => [...set]));
}
