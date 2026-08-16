export type KafkaJSErrorName =
  | 'KafkaJSError'
  | 'KafkaJSNonRetriableError'
  | 'KafkaJSProtocolError'
  | 'KafkaJSOffsetOutOfRange'
  | 'KafkaJSMemberIdRequired'
  | 'KafkaJSNumberOfRetriesExceeded'
  | 'KafkaJSConnectionError'
  | 'KafkaJSConnectionClosedError'
  | 'KafkaJSRequestTimeoutError'
  | 'KafkaJSMetadataNotLoaded'
  | 'KafkaJSTopicMetadataNotLoaded'
  | 'KafkaJSStaleTopicMetadataAssignment'
  | 'KafkaJSDeleteGroupsError'
  | 'KafkaJSServerDoesNotSupportApiKey'
  | 'KafkaJSBrokerNotFound'
  | 'KafkaJSPartialMessageError'
  | 'KafkaJSSASLAuthenticationError'
  | 'KafkaJSGroupCoordinatorNotFound'
  | 'KafkaJSNotImplemented'
  | 'KafkaJSTimeout'
  | 'KafkaJSLockTimeout'
  | 'KafkaJSUnsupportedMagicByteInMessageSet'
  | 'KafkaJSDeleteTopicRecordsError'
  | 'KafkaJSInvariantViolation'
  | 'KafkaJSInvalidVarIntError'
  | 'KafkaJSInvalidLongError'
  | 'KafkaJSCreateTopicError'
  | 'KafkaJSAggregateError'
  | 'KafkaJSFetcherRebalanceError'
  | 'KafkaJSNoBrokerAvailableError'
  | 'KafkaJSAlterPartitionReassignmentsError';

export interface KafkaJSErrorOptions {
  retriable?: boolean;
  cause?: unknown;
}

/**
 * Anything with a `.message`, e.g. a real `Error` or a plain protocol error-code descriptor
 * (`{ type, code, retriable, message }`, see `protocol/error-codes.ts`). Also carries the
 * optional `helpUrl` some error codes attach.
 */
type ErrorLike = { message: string; helpUrl?: string; stack?: string };

export class KafkaJSError extends Error {
  override readonly name: KafkaJSErrorName = 'KafkaJSError';
  readonly retriable: boolean;
  readonly helpUrl: string | undefined;
  override readonly cause: unknown;

  constructor(e: string | ErrorLike, { retriable = true, cause }: KafkaJSErrorOptions = {}) {
    const message = typeof e === 'string' ? e : e.message;
    super(message, { cause });
    Error.captureStackTrace(this, this.constructor);
    this.retriable = retriable;
    this.helpUrl = typeof e === 'string' ? undefined : e.helpUrl;
    this.cause = cause;
  }
}

export class KafkaJSNonRetriableError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSNonRetriableError';

  constructor(e: string | ErrorLike, { cause }: { cause?: unknown } = {}) {
    super(e, { retriable: false, cause });
  }
}

export interface KafkaJSProtocolErrorOptions {
  retriable?: boolean;
}

export class KafkaJSProtocolError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSProtocolError';
  readonly type: string | undefined;
  readonly code: number | undefined;

  constructor(
    e: ErrorLike & { retriable?: boolean; type?: string; code?: number },
    options: KafkaJSProtocolErrorOptions = {},
  ) {
    super(e, { retriable: options.retriable ?? e.retriable });
    this.type = e.type;
    this.code = e.code;
  }
}

export class KafkaJSOffsetOutOfRange extends KafkaJSProtocolError {
  override readonly name: KafkaJSErrorName = 'KafkaJSOffsetOutOfRange';
  readonly topic: string | undefined;
  readonly partition: number | undefined;

  constructor(
    e: ConstructorParameters<typeof KafkaJSProtocolError>[0],
    { topic, partition }: { topic?: string; partition?: number },
  ) {
    super(e);
    this.topic = topic;
    this.partition = partition;
  }
}

export class KafkaJSMemberIdRequired extends KafkaJSProtocolError {
  override readonly name: KafkaJSErrorName = 'KafkaJSMemberIdRequired';
  readonly memberId: string | undefined;

  constructor(e: ConstructorParameters<typeof KafkaJSProtocolError>[0], { memberId }: { memberId?: string }) {
    super(e);
    this.memberId = memberId;
  }
}

export class KafkaJSNumberOfRetriesExceeded extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSNumberOfRetriesExceeded';
  readonly retryCount: number;
  readonly retryTime: number;

  constructor(e: ErrorLike, { retryCount, retryTime }: { retryCount: number; retryTime: number }) {
    super(e, { cause: e });
    this.stack = `${this.name}\n  Caused by: ${e.stack}`;
    this.retryCount = retryCount;
    this.retryTime = retryTime;
  }
}

export class KafkaJSConnectionError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSConnectionError';
  readonly broker: string | undefined;
  readonly code: string | undefined;

  constructor(e: string | ErrorLike, { broker, code }: { broker?: string; code?: string } = {}) {
    super(e);
    this.broker = broker;
    this.code = code;
  }
}

export class KafkaJSConnectionClosedError extends KafkaJSConnectionError {
  override readonly name: KafkaJSErrorName = 'KafkaJSConnectionClosedError';
  readonly host: string | undefined;
  readonly port: number | undefined;

  constructor(e: string | ErrorLike, { host, port }: { host?: string; port?: number } = {}) {
    super(e, { broker: `${host}:${port}` });
    this.host = host;
    this.port = port;
  }
}

export class KafkaJSRequestTimeoutError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSRequestTimeoutError';
  readonly broker: string | undefined;
  readonly correlationId: number | undefined;
  readonly createdAt: number | undefined;
  readonly sentAt: number | undefined;
  readonly pendingDuration: number | undefined;

  constructor(
    e: string | ErrorLike,
    {
      broker,
      correlationId,
      createdAt,
      sentAt,
      pendingDuration,
    }: {
      broker?: string;
      correlationId?: number;
      createdAt?: number;
      sentAt?: number;
      pendingDuration?: number;
    } = {},
  ) {
    super(e);
    this.broker = broker;
    this.correlationId = correlationId;
    this.createdAt = createdAt;
    this.sentAt = sentAt;
    this.pendingDuration = pendingDuration;
  }
}

export class KafkaJSMetadataNotLoaded extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSMetadataNotLoaded';
}

export class KafkaJSTopicMetadataNotLoaded extends KafkaJSMetadataNotLoaded {
  override readonly name: KafkaJSErrorName = 'KafkaJSTopicMetadataNotLoaded';
  readonly topic: string | undefined;

  constructor(e: string | ErrorLike, { topic }: { topic?: string } = {}) {
    super(e);
    this.topic = topic;
  }
}

export class KafkaJSStaleTopicMetadataAssignment extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSStaleTopicMetadataAssignment';
  readonly topic: string | undefined;
  readonly unknownPartitions: unknown;

  constructor(
    e: string | ErrorLike,
    { topic, unknownPartitions }: { topic?: string; unknownPartitions?: unknown } = {},
  ) {
    super(e);
    this.topic = topic;
    this.unknownPartitions = unknownPartitions;
  }
}

export interface DeleteGroupError {
  groupId: string;
  errorCode?: number;
  error?: KafkaJSError;
}

export class KafkaJSDeleteGroupsError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSDeleteGroupsError';
  readonly groups: readonly DeleteGroupError[];

  constructor(e: string | ErrorLike, groups: readonly DeleteGroupError[] = []) {
    super(e);
    this.groups = groups;
  }
}

export class KafkaJSServerDoesNotSupportApiKey extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSServerDoesNotSupportApiKey';
  readonly apiKey: number | undefined;
  readonly apiName: string | undefined;

  constructor(e: string | ErrorLike, { apiKey, apiName }: { apiKey?: number; apiName?: string } = {}) {
    super(e);
    this.apiKey = apiKey;
    this.apiName = apiName;
  }
}

export class KafkaJSBrokerNotFound extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSBrokerNotFound';
}

export class KafkaJSPartialMessageError extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSPartialMessageError';
}

export class KafkaJSSASLAuthenticationError extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSSASLAuthenticationError';
}

export class KafkaJSGroupCoordinatorNotFound extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSGroupCoordinatorNotFound';
}

export class KafkaJSNotImplemented extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSNotImplemented';
}

export class KafkaJSTimeout extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSTimeout';
}

export class KafkaJSLockTimeout extends KafkaJSTimeout {
  override readonly name: KafkaJSErrorName = 'KafkaJSLockTimeout';
}

export class KafkaJSUnsupportedMagicByteInMessageSet extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSUnsupportedMagicByteInMessageSet';
}

export interface DeleteTopicRecordPartition {
  partition: number;
  error?: { retriable?: boolean };
  [key: string]: unknown;
}

export class KafkaJSDeleteTopicRecordsError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSDeleteTopicRecordsError';
  readonly partitions: readonly DeleteTopicRecordPartition[];

  constructor({ partitions }: { partitions: readonly DeleteTopicRecordPartition[] }) {
    const retriable = partitions.filter(({ error }) => error != null).every(({ error }) => error?.retriable === true);

    super('Error while deleting records', { retriable });
    this.partitions = partitions;
  }
}

const isErrorLike = (e: unknown): e is ErrorLike => e instanceof Error;

export class KafkaJSInvariantViolation extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSInvariantViolation';

  constructor(e: string | ErrorLike) {
    const message = isErrorLike(e) ? e.message : e;
    super(`Invariant violated: ${message}. This is likely a bug and should be reported.`);
  }
}

export class KafkaJSInvalidVarIntError extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSInvalidVarIntError';
}

export class KafkaJSInvalidLongError extends KafkaJSNonRetriableError {
  override readonly name: KafkaJSErrorName = 'KafkaJSInvalidLongError';
}

export class KafkaJSCreateTopicError extends KafkaJSProtocolError {
  override readonly name: KafkaJSErrorName = 'KafkaJSCreateTopicError';
  readonly topic: string;

  constructor(e: ConstructorParameters<typeof KafkaJSProtocolError>[0], topicName: string) {
    super(e);
    this.topic = topicName;
  }
}

export class KafkaJSAlterPartitionReassignmentsError extends KafkaJSProtocolError {
  override readonly name: KafkaJSErrorName = 'KafkaJSAlterPartitionReassignmentsError';
  readonly topic: string;
  readonly partition: number;

  constructor(e: ConstructorParameters<typeof KafkaJSProtocolError>[0], topicName: string, partition: number) {
    super(e);
    this.topic = topicName;
    this.partition = partition;
  }
}

export class KafkaJSAggregateError extends Error {
  override readonly name: KafkaJSErrorName = 'KafkaJSAggregateError';
  readonly errors: readonly unknown[];

  constructor(message: string, errors: readonly unknown[]) {
    super(message);
    this.errors = errors;
  }
}

export class KafkaJSFetcherRebalanceError extends Error {
  override readonly name: KafkaJSErrorName = 'KafkaJSFetcherRebalanceError';
}

export class KafkaJSNoBrokerAvailableError extends KafkaJSError {
  override readonly name: KafkaJSErrorName = 'KafkaJSNoBrokerAvailableError';

  constructor() {
    super('No broker available');
  }
}

export const isRebalancing = (e: { type?: string }): boolean =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP' || e.type === 'ILLEGAL_GENERATION';

export const isKafkaJSError = (e: unknown): e is KafkaJSError => e instanceof KafkaJSError;
