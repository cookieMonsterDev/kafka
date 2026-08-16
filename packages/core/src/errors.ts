export type KafkaErrorName =
  | 'KafkaError'
  | 'KafkaNonRetriableError'
  | 'KafkaProtocolError'
  | 'KafkaOffsetOutOfRange'
  | 'KafkaMemberIdRequired'
  | 'KafkaNumberOfRetriesExceeded'
  | 'KafkaConnectionError'
  | 'KafkaConnectionClosedError'
  | 'KafkaRequestTimeoutError'
  | 'KafkaMetadataNotLoaded'
  | 'KafkaTopicMetadataNotLoaded'
  | 'KafkaStaleTopicMetadataAssignment'
  | 'KafkaDeleteGroupsError'
  | 'KafkaServerDoesNotSupportApiKey'
  | 'KafkaBrokerNotFound'
  | 'KafkaPartialMessageError'
  | 'KafkaSASLAuthenticationError'
  | 'KafkaGroupCoordinatorNotFound'
  | 'KafkaNotImplemented'
  | 'KafkaTimeout'
  | 'KafkaLockTimeout'
  | 'KafkaUnsupportedMagicByteInMessageSet'
  | 'KafkaDeleteTopicRecordsError'
  | 'KafkaInvariantViolation'
  | 'KafkaInvalidVarIntError'
  | 'KafkaInvalidLongError'
  | 'KafkaCreateTopicError'
  | 'KafkaAggregateError'
  | 'KafkaFetcherRebalanceError'
  | 'KafkaNoBrokerAvailableError'
  | 'KafkaAlterPartitionReassignmentsError';

export interface KafkaErrorOptions {
  retriable?: boolean;
  cause?: unknown;
}

/**
 * Anything with a `.message`, e.g. a real `Error` or a plain protocol error-code descriptor
 * (`{ type, code, retriable, message }`, see `protocol/error-codes.ts`). Also carries the
 * optional `helpUrl` some error codes attach.
 */
type ErrorLike = { message: string; helpUrl?: string; stack?: string };

/**
 * Base client error. `retriable` is true when the operation can be safely retried.
 * @see https://kafka.apache.org/43/design/protocol/
 */
export class KafkaError extends Error {
  override readonly name: KafkaErrorName = 'KafkaError';
  readonly retriable: boolean;
  readonly helpUrl: string | undefined;
  override readonly cause: unknown;

  constructor(e: string | ErrorLike, { retriable = true, cause }: KafkaErrorOptions = {}) {
    const message = typeof e === 'string' ? e : e.message;
    super(message, { cause });
    Error.captureStackTrace(this, this.constructor);
    this.retriable = retriable;
    this.helpUrl = typeof e === 'string' ? undefined : e.helpUrl;
    this.cause = cause;
  }
}

/** Error that must not be retried (invalid arguments, unimplemented APIs, auth failures). */
export class KafkaNonRetriableError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaNonRetriableError';

  constructor(e: string | ErrorLike, { cause }: { cause?: unknown } = {}) {
    super(e, { retriable: false, cause });
  }
}

export interface KafkaProtocolErrorOptions {
  retriable?: boolean;
  topic?: string;
  partition?: number;
}

/**
 * Broker error-code response (`type`, `code`, `retriable`).
 * @see https://kafka.apache.org/43/design/protocol/
 */
export class KafkaProtocolError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaProtocolError';
  readonly type: string | undefined;
  readonly code: number | undefined;
  readonly topic: string | undefined;
  readonly partition: number | undefined;

  constructor(
    e: ErrorLike & { retriable?: boolean; type?: string; code?: number },
    options: KafkaProtocolErrorOptions = {},
  ) {
    const topic = options.topic;
    const partition = options.partition;
    const details = [
      topic != null ? `topic: ${topic}` : null,
      partition != null ? `partition: ${partition}` : null,
    ].filter((part): part is string => part != null);
    const message = details.length > 0 ? `${e.message} (${details.join(', ')})` : e.message;
    super({ ...e, message }, { retriable: options.retriable ?? e.retriable });
    this.type = e.type;
    this.code = e.code;
    this.topic = topic;
    this.partition = partition;
  }
}

export class KafkaOffsetOutOfRange extends KafkaProtocolError {
  override readonly name: KafkaErrorName = 'KafkaOffsetOutOfRange';
  override readonly topic: string | undefined;
  override readonly partition: number | undefined;

  constructor(
    e: ConstructorParameters<typeof KafkaProtocolError>[0],
    { topic, partition }: { topic?: string; partition?: number },
  ) {
    super(e);
    this.topic = topic;
    this.partition = partition;
  }
}

export class KafkaMemberIdRequired extends KafkaProtocolError {
  override readonly name: KafkaErrorName = 'KafkaMemberIdRequired';
  readonly memberId: string | undefined;

  constructor(e: ConstructorParameters<typeof KafkaProtocolError>[0], { memberId }: { memberId?: string }) {
    super(e);
    this.memberId = memberId;
  }
}

export class KafkaNumberOfRetriesExceeded extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaNumberOfRetriesExceeded';
  readonly retryCount: number;
  readonly retryTime: number;

  constructor(e: ErrorLike, { retryCount, retryTime }: { retryCount: number; retryTime: number }) {
    super(e, { cause: e });
    this.stack = `${this.name}\n  Caused by: ${e.stack}`;
    this.retryCount = retryCount;
    this.retryTime = retryTime;
  }
}

export class KafkaConnectionError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaConnectionError';
  readonly broker: string | undefined;
  readonly code: string | undefined;

  constructor(e: string | ErrorLike, { broker, code }: { broker?: string; code?: string } = {}) {
    super(e);
    this.broker = broker;
    this.code = code;
  }
}

export class KafkaConnectionClosedError extends KafkaConnectionError {
  override readonly name: KafkaErrorName = 'KafkaConnectionClosedError';
  readonly host: string | undefined;
  readonly port: number | undefined;

  constructor(e: string | ErrorLike, { host, port }: { host?: string; port?: number } = {}) {
    super(e, { broker: `${host}:${port}` });
    this.host = host;
    this.port = port;
  }
}

export class KafkaRequestTimeoutError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaRequestTimeoutError';
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

export class KafkaMetadataNotLoaded extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaMetadataNotLoaded';
}

export class KafkaTopicMetadataNotLoaded extends KafkaMetadataNotLoaded {
  override readonly name: KafkaErrorName = 'KafkaTopicMetadataNotLoaded';
  readonly topic: string | undefined;

  constructor(e: string | ErrorLike, { topic }: { topic?: string } = {}) {
    super(e);
    this.topic = topic;
  }
}

export class KafkaStaleTopicMetadataAssignment extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaStaleTopicMetadataAssignment';
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
  error?: KafkaError;
}

export class KafkaDeleteGroupsError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaDeleteGroupsError';
  readonly groups: readonly DeleteGroupError[];

  constructor(e: string | ErrorLike, groups: readonly DeleteGroupError[] = []) {
    super(e);
    this.groups = groups;
  }
}

export interface KafkaServerDoesNotSupportApiKeyOptions {
  apiKey?: number;
  apiName?: string;
  brokerMinVersion?: number;
  brokerMaxVersion?: number;
  implementedVersions?: readonly number[];
}

export class KafkaServerDoesNotSupportApiKey extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaServerDoesNotSupportApiKey';
  readonly apiKey: number | undefined;
  readonly apiName: string | undefined;
  readonly brokerMinVersion: number | undefined;
  readonly brokerMaxVersion: number | undefined;
  readonly implementedVersions: readonly number[] | undefined;

  constructor(e: string | ErrorLike, options: KafkaServerDoesNotSupportApiKeyOptions = {}) {
    super(e);
    this.apiKey = options.apiKey;
    this.apiName = options.apiName;
    this.brokerMinVersion = options.brokerMinVersion;
    this.brokerMaxVersion = options.brokerMaxVersion;
    this.implementedVersions = options.implementedVersions;
  }
}

export class KafkaBrokerNotFound extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaBrokerNotFound';
}

export class KafkaPartialMessageError extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaPartialMessageError';
}

export class KafkaSASLAuthenticationError extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaSASLAuthenticationError';
}

export class KafkaGroupCoordinatorNotFound extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaGroupCoordinatorNotFound';
}

export class KafkaNotImplemented extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaNotImplemented';
}

export class KafkaTimeout extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaTimeout';
}

export class KafkaLockTimeout extends KafkaTimeout {
  override readonly name: KafkaErrorName = 'KafkaLockTimeout';
}

export class KafkaUnsupportedMagicByteInMessageSet extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaUnsupportedMagicByteInMessageSet';
}

export interface DeleteTopicRecordPartition {
  partition: number;
  error?: { retriable?: boolean };
  [key: string]: unknown;
}

export class KafkaDeleteTopicRecordsError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaDeleteTopicRecordsError';
  readonly partitions: readonly DeleteTopicRecordPartition[];

  constructor({ partitions }: { partitions: readonly DeleteTopicRecordPartition[] }) {
    const retriable = partitions.filter(({ error }) => error != null).every(({ error }) => error?.retriable === true);

    super('Error while deleting records', { retriable });
    this.partitions = partitions;
  }
}

const isErrorLike = (e: unknown): e is ErrorLike => e instanceof Error;

export class KafkaInvariantViolation extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaInvariantViolation';

  constructor(e: string | ErrorLike) {
    const message = isErrorLike(e) ? e.message : e;
    super(`Invariant violated: ${message}. This is likely a bug and should be reported.`);
  }
}

export class KafkaInvalidVarIntError extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaInvalidVarIntError';
}

export class KafkaInvalidLongError extends KafkaNonRetriableError {
  override readonly name: KafkaErrorName = 'KafkaInvalidLongError';
}

export class KafkaCreateTopicError extends KafkaProtocolError {
  override readonly name: KafkaErrorName = 'KafkaCreateTopicError';
  override readonly topic: string;

  constructor(e: ConstructorParameters<typeof KafkaProtocolError>[0], topicName: string) {
    super(e);
    this.topic = topicName;
  }
}

export class KafkaAlterPartitionReassignmentsError extends KafkaProtocolError {
  override readonly name: KafkaErrorName = 'KafkaAlterPartitionReassignmentsError';
  override readonly topic: string;
  override readonly partition: number;

  constructor(e: ConstructorParameters<typeof KafkaProtocolError>[0], topicName: string, partition: number) {
    super(e);
    this.topic = topicName;
    this.partition = partition;
  }
}

export class KafkaAggregateError extends Error {
  override readonly name: KafkaErrorName = 'KafkaAggregateError';
  readonly errors: readonly unknown[];

  constructor(message: string, errors: readonly unknown[]) {
    super(message);
    this.errors = errors;
  }
}

export class KafkaFetcherRebalanceError extends Error {
  override readonly name: KafkaErrorName = 'KafkaFetcherRebalanceError';
}

export class KafkaNoBrokerAvailableError extends KafkaError {
  override readonly name: KafkaErrorName = 'KafkaNoBrokerAvailableError';

  constructor() {
    super('No broker available');
  }
}

export const isRebalancing = (e: { type?: string }): boolean =>
  e.type === 'REBALANCE_IN_PROGRESS' || e.type === 'NOT_COORDINATOR_FOR_GROUP' || e.type === 'ILLEGAL_GENERATION';

export const isKafkaError = (e: unknown): e is KafkaError => e instanceof KafkaError;
