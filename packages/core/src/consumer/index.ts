import type { Cluster } from '../cluster/index';
import { EARLIEST_OFFSET, LATEST_OFFSET } from '../constants';
import { KafkaNonRetriableError } from '../errors';
import { InstrumentationEventEmitter, type RemoveInstrumentationEventListener } from '../instrumentation/emitter';
import type { InstrumentationEvent } from '../instrumentation/event';
import type { Logger } from '../loggers/index';
import { ISOLATION_LEVEL, type IsolationLevel } from '../protocol/enums/isolation-level';
import { retrier } from '../retry/index';
import { RETRY_DEFAULTS } from '../retry/defaults';
import { abortError, rejectOnAbort, type ConnectOptions } from '../utils/abort';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { sleep } from '../utils/wait';
import { roundRobin } from './assigners/index';
import type { Batch } from './batch';
import { ConsumerGroup } from './consumer-group';
import {
  CONNECT,
  CRASH,
  DISCONNECT,
  events,
  STOP,
  unwrap,
  wrap,
  type ConsumerEventName,
} from './instrumentation-events';
import {
  topicOffsetConfigurationFromSubscribe,
  type AutoOffsetReset,
  type TopicOffsetConfiguration,
} from './offset-reset';
import { Runner } from './runner';
import type {
  Assigner,
  ConsumerHooks,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  EachBatchHandler,
  GroupDescription,
  PartitionAssigner,
  TopicPartition,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
} from './types';
import { parseOffset } from './types';

export type { AutoOffsetReset, TopicOffsetConfiguration } from './offset-reset';
export type {
  Assigner,
  ConsumerHooks,
  ConsumerRetryOptions,
  ConsumerRunConfig,
  EachBatchHandler,
  EachBatchPayload,
  EachMessageHandler,
  EachMessagePayload,
  GroupDescription,
  KafkaMessage,
  OnCommitEvent,
  OnCommitHook,
  OnConsumeEvent,
  OnConsumeHook,
  PartitionAssigner,
  RebalanceListener,
  TopicPartition,
  TopicPartitionOffset,
  TopicPartitionOffsetAndMetadata,
  TopicPartitions,
} from './types';

export { events };
export { MemberAssignment, MemberMetadata } from './assigner-protocol';
export { cooperativeSticky, range, roundRobin, sticky } from './assigners/index';
export { Batch } from './batch';

/** Subscribe to several topics. @see https://kafka.apache.org/43/configuration/consumer-configs/ */
export interface ConsumerSubscribeTopics {
  topics: readonly (string | RegExp)[];
  /** When true, start from the earliest offset if the group has no committed position. */
  fromBeginning?: boolean;
  /**
   * Offset reset policy when there is no committed offset (Java `auto.offset.reset`).
   * Wins over `fromBeginning` when set. `none` throws instead of resetting.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
   */
  autoOffsetReset?: AutoOffsetReset;
}

export interface ConsumerSubscribeTopic {
  topic: string | RegExp;
  fromBeginning?: boolean;
  /**
   * Offset reset policy when there is no committed offset (Java `auto.offset.reset`).
   * Wins over `fromBeginning` when set. `none` throws instead of resetting.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
   */
  autoOffsetReset?: AutoOffsetReset;
}

export interface ConsumerOptions {
  cluster: Cluster;
  /**
   * Consumer group id. Required to use {@link Consumer.subscribe}. Optional for
   * {@link Consumer.assign}, which fetches without group membership; configure it there only if
   * you plan to call {@link Consumer.commitOffsets}.
   */
  groupId?: string;
  retry?: ConsumerRetryOptions;
  logger: Logger;
  partitionAssigners?: PartitionAssigner[];
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  heartbeatInterval?: number;
  maxBytesPerPartition?: number;
  minBytes?: number;
  maxBytes?: number;
  maxWaitTimeInMs?: number;
  isolationLevel?: IsolationLevel;
  rackId?: string;
  instrumentationEmitter?: InstrumentationEventEmitter | null;
  metadataMaxAge?: number;
  groupInstanceId?: string;
  /**
   * Default offset reset policy for subscriptions that omit `autoOffsetReset`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
   */
  autoOffsetReset?: AutoOffsetReset;
  /**
   * Group membership protocol. `'classic'` (default) uses JoinGroup/SyncGroup.
   * `'consumer'` opts into KIP-848 ConsumerGroupHeartbeat (Kafka 4.0+). Java name:
   * `group.protocol`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.protocol
   */
  groupProtocol?: 'classic' | 'consumer';
  /**
   * Server-side partition assignor to request under the KIP-848 consumer protocol. Only
   * meaningful when `groupProtocol` is `'consumer'`; ignored (logged at debug level) otherwise,
   * the same as `sessionTimeout`, `heartbeatInterval`, and `partitionAssigners` are unused under
   * `groupProtocol: 'consumer'`. Broker property: `group.remote.assignor`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#group.remote.assignor
   */
  groupRemoteAssignor?: 'uniform' | 'range';
  /** Ordered async `onConsume`/`onCommit` hooks. See {@link ConsumerHooks}. */
  hooks?: ConsumerHooks;
  /**
   * Verify each fetched record batch's CRC on decode. Default `true`.
   * @see https://kafka.apache.org/43/configuration/consumer-configs/#check.crcs
   */
  checkCrcs?: boolean;
}

/**
 * Consumer returned by {@link Kafka.consumer}.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export interface Consumer {
  connect: (options?: ConnectOptions) => Promise<void>;
  disconnect: (options?: ConnectOptions) => Promise<void>;
  subscribe: (subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic) => Promise<void>;
  /**
   * Assign exact partitions to fetch, with no group membership: no JoinGroup/SyncGroup,
   * ConsumerGroupHeartbeat, or rebalancing. Mutually exclusive with {@link Consumer.subscribe} -
   * calling one after the other throws. `groupId` is not required unless you also call
   * {@link Consumer.commitOffsets}. See the "Assign mode" guide for the offset-commit policy.
   */
  assign: (topicPartitions: readonly { topic: string; partition: number }[]) => Promise<void>;
  stop: () => Promise<void>;
  run: (config?: ConsumerRunConfig) => Promise<void>;
  /** Async iteration over fetched batches. Cannot run alongside {@link Consumer.run}. */
  stream: (config?: Omit<ConsumerRunConfig, 'eachBatch' | 'eachMessage'>) => AsyncIterableIterator<Batch>;
  commitOffsets: (topicPartitions: readonly TopicPartitionOffsetAndMetadata[]) => Promise<void>;
  seek: (topicPartitionOffset: { topic: string; partition: number; offset: bigint | number | string }) => void;
  describeGroup: () => Promise<GroupDescription>;
  /**
   * Committed offsets for the given partitions, read from the group coordinator via OffsetFetch.
   * This queries the broker directly - it does not require `run()`/`stream()`/`assign()` to have
   * started. A partition with no committed offset comes back with `offset: -1n` (Kafka's wire
   * convention for "none") and `metadata: null`, matching what the broker returns.
   *
   * This is the live-consumer counterpart to `Admin.fetchOffsets`, which stays the out-of-band
   * tool for inspecting/resetting offsets without a running consumer.
   */
  committed: (topicPartitions: readonly TopicPartition[]) => Promise<TopicPartitionOffsetAndMetadata[]>;
  /**
   * Next fetch offset for an assigned partition (the offset of the next record this consumer
   * would read). Returns `null` when the partition is not currently assigned to this consumer -
   * rather than throwing - since that is an expected, common state (e.g. right after a rebalance).
   * Throws {@link KafkaNonRetriableError} if the group/assignment has not started yet
   * (`run()`/`stream()`/`assign()`).
   */
  position: (topicPartition: TopicPartition) => bigint | null;
  /**
   * `highWatermark - position` for a partition. Returns `null` when the partition is not
   * currently assigned, or when no Fetch response has been seen for it yet (so there is no known
   * high watermark) - the same "unknown yet, don't throw" contract as {@link Consumer.position}.
   * Throws {@link KafkaNonRetriableError} if the group/assignment has not started yet.
   */
  currentLag: (topicPartition: TopicPartition) => bigint | null;
  pause: (topics: readonly { topic: string; partitions?: number[] }[]) => void;
  paused: () => TopicPartitions[];
  resume: (topics: readonly { topic: string; partitions?: number[] }[]) => void;
  on: (
    eventName: ConsumerEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ) => RemoveInstrumentationEventListener;
  readonly events: typeof events;
  logger: () => Logger;
  [Symbol.asyncDispose]: () => Promise<void>;
}

const EVENT_NAMES: ReadonlySet<string> = new Set(Object.values(events));
const EVENT_KEYS = Object.keys(events)
  .map((key) => `consumer.events.${key}`)
  .join(', ');

const SPECIAL_OFFSETS = new Set([BigInt(EARLIEST_OFFSET), BigInt(LATEST_OFFSET)]);

/**
 * User-facing consumer: group membership, fetch/process loop, pause/resume/seek, and events.
 *
 * @see https://kafka.apache.org/43/configuration/consumer-configs/
 */
export function createConsumer({
  cluster,
  groupId,
  retry,
  logger: rootLogger,
  partitionAssigners = [roundRobin],
  sessionTimeout = 30_000,
  rebalanceTimeout = 60_000,
  heartbeatInterval = 3_000,
  maxBytesPerPartition = 1_048_576,
  minBytes = 1,
  maxBytes = 10_485_760,
  maxWaitTimeInMs = 5_000,
  isolationLevel = ISOLATION_LEVEL.READ_COMMITTED,
  rackId = '',
  instrumentationEmitter: rootInstrumentationEmitter,
  metadataMaxAge = 300_000,
  groupInstanceId,
  autoOffsetReset,
  groupProtocol = 'classic',
  groupRemoteAssignor,
  hooks,
  checkCrcs = true,
}: ConsumerOptions): Consumer {
  const logger = rootLogger.namespace('Consumer');
  const instrumentationEmitter = rootInstrumentationEmitter ?? new InstrumentationEventEmitter();
  const assigners: Assigner[] = partitionAssigners.map((createAssigner) =>
    createAssigner({ groupId: groupId ?? '', logger, cluster }),
  );

  const topics: Record<string, TopicOffsetConfiguration> = {};
  // KIP-848 server-side regex subscription (`groupProtocol: 'consumer'` only): the `.source` of
  // the single RegExp subscribed so far, sent as `subscribedTopicRegex` instead of a client-
  // resolved topic list, plus the offset-reset config to apply to topics it matches.
  let subscribedTopicRegex: string | null = null;
  let subscribedTopicRegexConfiguration: TopicOffsetConfiguration | null = null;
  let runner: Runner | null = null;
  let consumerGroup: ConsumerGroup | null = null;
  let restartTimeout: ReturnType<typeof setTimeout> | null = null;

  /**
   * `subscribe()` (group membership, rebalanced assignment) and `assign()` (fixed partitions,
   * no group membership) are mutually exclusive; `run`/`stream` require one of them first.
   */
  let mode: 'unset' | 'subscribe' | 'assign' = 'unset';
  let assignment: TopicPartitions[] = [];

  if (groupId === '') {
    throw new KafkaNonRetriableError('Consumer groupId must be a non-empty string.');
  }

  if (groupProtocol !== 'consumer' && heartbeatInterval >= sessionTimeout) {
    throw new KafkaNonRetriableError(
      `Consumer heartbeatInterval (${heartbeatInterval}) must be lower than sessionTimeout (${sessionTimeout}). It is recommended to set heartbeatInterval to approximately a third of the sessionTimeout.`,
    );
  }

  const connect = async ({ signal }: ConnectOptions = {}): Promise<void> => {
    if (signal?.aborted) throw abortError(signal);
    await rejectOnAbort(cluster.connect(), signal);
    instrumentationEmitter.emit(CONNECT, {});
  };

  const stop = sharedPromiseTo(async (): Promise<void> => {
    try {
      if (runner) {
        await runner.stop();
        runner = null;
        consumerGroup = null;
        instrumentationEmitter.emit(STOP, {});
      }

      if (restartTimeout) clearTimeout(restartTimeout);
      logger.info('Stopped', { groupId });
    } catch (e) {
      const error = e as Error;
      logger.error(`Caught error when stopping the consumer: ${error.message}`, {
        stack: error.stack,
        groupId,
      });
      throw error;
    }
  });

  const disconnect = async ({ signal }: ConnectOptions = {}): Promise<void> => {
    if (signal?.aborted) throw abortError(signal);
    try {
      await rejectOnAbort(stop(), signal);
      logger.debug('consumer has stopped, disconnecting', { groupId });
      await rejectOnAbort(cluster.disconnect(), signal);
      instrumentationEmitter.emit(DISCONNECT, {});
    } catch (e) {
      const error = e as Error;
      logger.error(`Caught error when disconnecting the consumer: ${error.message}`, {
        stack: error.stack,
        groupId,
      });
      throw error;
    }
  };

  const subscribe = async (subscription: ConsumerSubscribeTopics | ConsumerSubscribeTopic): Promise<void> => {
    if (mode === 'assign') {
      throw new KafkaNonRetriableError(
        'Cannot call subscribe() after assign(); a consumer must use either subscribe() or assign(), not both.',
      );
    }

    if (consumerGroup) {
      throw new KafkaNonRetriableError('Cannot subscribe to topic while consumer is running');
    }

    if (!groupId) {
      throw new KafkaNonRetriableError('Consumer groupId must be a non-empty string to use subscribe().');
    }

    mode = 'subscribe';

    const topicConfiguration = topicOffsetConfigurationFromSubscribe(subscription, autoOffsetReset);
    const isTopicName = (entry: unknown): entry is string | RegExp =>
      typeof entry === 'string' || entry instanceof RegExp;
    const isTopicNameList = (value: unknown): value is readonly unknown[] => Array.isArray(value);

    const subscriptions: (string | RegExp)[] = [];
    if ('topics' in subscription) {
      if (!isTopicNameList(subscription.topics)) {
        throw new KafkaNonRetriableError('Argument "topics" must be an array');
      }
      for (const entry of subscription.topics) {
        if (!isTopicName(entry)) {
          throw new KafkaNonRetriableError(
            `Invalid topic ${String(entry)} (${typeof entry}), the topic name has to be a String or a RegExp`,
          );
        }
        subscriptions.push(entry);
      }
    } else if (subscription.topic != null) {
      if (!isTopicName(subscription.topic)) {
        throw new KafkaNonRetriableError(
          `Invalid topic ${String(subscription.topic)} (${typeof subscription.topic}), the topic name has to be a String or a RegExp`,
        );
      }
      subscriptions.push(subscription.topic);
    } else {
      throw new KafkaNonRetriableError('Missing required argument "topics"');
    }

    const regexSubscriptions = subscriptions.filter((entry): entry is RegExp => entry instanceof RegExp);
    const nameSubscriptions = subscriptions.filter((entry): entry is string => typeof entry === 'string');

    if (groupProtocol === 'consumer' && regexSubscriptions.length > 0) {
      // KIP-848: the broker matches a `SubscriptionPattern` server-side instead of the client
      // scanning metadata itself, but a member can only carry one such pattern - reject a second,
      // distinct RegExp across this and any earlier subscribe() call in this consumer's lifetime.
      const alreadyDiffers =
        subscribedTopicRegex != null && regexSubscriptions.some((entry) => entry.source !== subscribedTopicRegex);
      if (regexSubscriptions.length > 1 || alreadyDiffers) {
        throw new KafkaNonRetriableError(
          'Only one RegExp subscription is supported per consumer group when groupProtocol is "consumer" ' +
            '(KIP-848 subscribes via a single server-side SubscriptionPattern per member)',
        );
      }

      const [pattern] = regexSubscriptions;
      subscribedTopicRegex = pattern!.source;
      subscribedTopicRegexConfiguration = topicConfiguration;

      logger.debug('Subscription based on RegExp will be matched server-side (KIP-848 SubscriptionPattern)', {
        groupId,
        topicRegExp: pattern!.toString(),
      });

      for (const name of nameSubscriptions) {
        topics[name] = topicConfiguration;
      }

      await cluster.addMultipleTargetTopics(nameSubscriptions);
      return;
    }

    const hasRegexSubscriptions = regexSubscriptions.length > 0;
    const regexEntries = regexSubscriptions;

    const matchTopicsForRegex = (
      candidateMetadata: Awaited<ReturnType<typeof cluster.metadata>> | undefined,
      entry: RegExp,
    ): string[] =>
      (candidateMetadata?.topicMetadata ?? [])
        .map(({ topic: topicName }) => topicName)
        .filter((topicName): topicName is string => topicName != null)
        .filter((topicName) => {
          // `RegExp.test` with the global flag advances `lastIndex` and skips later matches.
          const matcher = entry.flags.includes('g') ? new RegExp(entry.source, entry.flags.replaceAll('g', '')) : entry;
          return matcher.test(topicName);
        });

    let metadata = hasRegexSubscriptions ? await cluster.metadata() : undefined;

    if (regexEntries.length > 0) {
      const collectMatches = (candidateMetadata: typeof metadata): Set<string> => {
        const matched = new Set<string>();
        for (const entry of regexEntries) {
          for (const topic of matchTopicsForRegex(candidateMetadata, entry)) matched.add(topic);
        }
        return matched;
      };

      // A topic created just before subscribe() propagates to the broker serving this metadata
      // request through Kafka's own inter-broker metadata propagation, not through anything this
      // client controls, so a regex subscription can match some but not all of the intended
      // topics on the first few reads. Keep forcing a refresh until two consecutive reads agree
      // on the same set, or a bounded number of attempts pass.
      const REGEX_METADATA_MAX_ATTEMPTS = 20;
      const REGEX_METADATA_REQUIRED_STABLE_READS = 2;
      let previousMatches = collectMatches(metadata);
      let stableReads = 0;
      for (
        let attempt = 0;
        attempt < REGEX_METADATA_MAX_ATTEMPTS && stableReads < REGEX_METADATA_REQUIRED_STABLE_READS;
        attempt++
      ) {
        await sleep(150);
        metadata = await cluster.metadata({ forceRefresh: true });
        const nextMatches = collectMatches(metadata);
        const isStable =
          nextMatches.size === previousMatches.size && [...nextMatches].every((topic) => previousMatches.has(topic));
        previousMatches = nextMatches;
        stableReads = isStable ? stableReads + 1 : 0;
      }
    }

    const topicsToSubscribe: string[] = [];
    for (const entry of subscriptions) {
      if (entry instanceof RegExp) {
        const matchedTopics = matchTopicsForRegex(metadata, entry);

        logger.debug('Subscription based on RegExp', {
          groupId,
          topicRegExp: entry.toString(),
          matchedTopics,
        });

        topicsToSubscribe.push(...matchedTopics);
      } else {
        topicsToSubscribe.push(entry);
      }
    }

    for (const name of topicsToSubscribe) {
      topics[name] = topicConfiguration;
    }

    await cluster.addMultipleTargetTopics(topicsToSubscribe);
  };

  /**
   * Assign exact partitions to fetch, skipping group membership entirely: no JoinGroup/SyncGroup,
   * no ConsumerGroupHeartbeat, no rebalancing. Mutually exclusive with {@link subscribe}.
   *
   * Initial position per partition (see the "Assign mode" guide for the full policy): an earlier
   * {@link seek} always wins; otherwise, when `groupId` is configured, the group's committed
   * offset if one exists; otherwise `autoOffsetReset` (default `latest`).
   */
  const assign = async (topicPartitions: readonly { topic: string; partition: number }[]): Promise<void> => {
    if (mode === 'subscribe') {
      throw new KafkaNonRetriableError(
        'Cannot call assign() after subscribe(); a consumer must use either subscribe() or assign(), not both.',
      );
    }

    if (consumerGroup) {
      throw new KafkaNonRetriableError('Cannot assign partitions while consumer is running');
    }

    const isTopicPartitionList = (value: unknown): value is readonly unknown[] => Array.isArray(value);
    if (!isTopicPartitionList(topicPartitions)) {
      throw new KafkaNonRetriableError('Argument "topicPartitions" must be an array');
    }

    const partitionsByTopic: Record<string, Set<number>> = {};
    for (const entry of topicPartitions) {
      const topic = entry?.topic;
      if (typeof topic !== 'string' || !topic) {
        throw new KafkaNonRetriableError(`Invalid topic ${String(topic)}`);
      }

      const partition = entry?.partition;
      if (typeof partition !== 'number' || Number.isNaN(partition)) {
        throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${String(partition)}`);
      }

      (partitionsByTopic[topic] ??= new Set()).add(partition);
    }

    mode = 'assign';

    const topicConfiguration: TopicOffsetConfiguration = autoOffsetReset != null ? { autoOffsetReset } : {};
    const topicNames = Object.keys(partitionsByTopic);
    for (const name of topicNames) {
      topics[name] = topicConfiguration;
    }

    assignment = topicNames.map((topic) => ({
      topic,
      partitions: [...(partitionsByTopic[topic] ?? [])].sort((a, b) => a - b),
    }));

    await cluster.addMultipleTargetTopics(topicNames);
  };

  const run = async ({
    autoCommit = true,
    autoCommitInterval = null,
    autoCommitThreshold = null,
    eachBatchAutoResolve = true,
    partitionsConsumedConcurrently: concurrency = 1,
    prefetchMaxBatches,
    prefetchMaxBytes,
    maxRecords,
    eachBatch = null,
    eachMessage = null,
    signal,
    onPartitionsRevoked,
    onPartitionsAssigned,
    onPartitionsLost,
  }: ConsumerRunConfig = {}): Promise<void> => {
    if (consumerGroup) {
      logger.warn('consumer#run was called, but the consumer is already running', { groupId });
      return;
    }

    if (signal?.aborted) {
      throw abortError(signal);
    }

    if (mode === 'unset') {
      throw new KafkaNonRetriableError('Consumer must call subscribe() or assign() before run().');
    }

    // assign() mode never auto-commits: there is no consumer group to own the offsets, so
    // committing only ever happens through an explicit commitOffsets() call (which itself
    // requires a configured groupId). autoCommit/autoCommitInterval/autoCommitThreshold from
    // `run()` are ignored in this mode.
    const runnerAutoCommit = mode === 'assign' ? false : autoCommit;
    if (mode === 'assign' && (autoCommitInterval != null || autoCommitThreshold != null)) {
      logger.warn(
        'autoCommitInterval/autoCommitThreshold have no effect in assign() mode; call commitOffsets() explicitly instead',
        { groupId },
      );
    }

    const start = async (onCrash: (reason: Error) => Promise<void>): Promise<void> => {
      logger.info('Starting', { groupId, mode });

      consumerGroup =
        mode === 'assign'
          ? new ConsumerGroup({
              logger: rootLogger,
              topics: assignment.map(({ topic }) => topic),
              topicConfigurations: topics,
              assignedPartitions: assignment,
              retry,
              cluster,
              groupId: groupId ?? null,
              assigners,
              sessionTimeout,
              rebalanceTimeout,
              maxBytesPerPartition,
              minBytes,
              maxBytes,
              maxWaitTimeInMs,
              instrumentationEmitter,
              isolationLevel,
              groupInstanceId,
              rackId,
              metadataMaxAge,
              autoCommit: false,
              autoCommitInterval,
              autoCommitThreshold,
              groupProtocol,
              groupRemoteAssignor,
              subscribedTopicRegex,
              defaultTopicConfiguration: subscribedTopicRegexConfiguration ?? undefined,
              hooks,
              checkCrcs,
              onPartitionsRevoked,
              onPartitionsAssigned,
              onPartitionsLost,
            })
          : new ConsumerGroup({
              logger: rootLogger,
              topics: Object.keys(topics),
              topicConfigurations: topics,
              retry,
              cluster,
              groupId: groupId ?? null,
              assigners,
              sessionTimeout,
              rebalanceTimeout,
              maxBytesPerPartition,
              minBytes,
              maxBytes,
              maxWaitTimeInMs,
              instrumentationEmitter,
              isolationLevel,
              groupInstanceId,
              rackId,
              metadataMaxAge,
              autoCommit,
              autoCommitInterval,
              autoCommitThreshold,
              groupProtocol,
              groupRemoteAssignor,
              subscribedTopicRegex,
              defaultTopicConfiguration: subscribedTopicRegexConfiguration ?? undefined,
              hooks,
              checkCrcs,
              onPartitionsRevoked,
              onPartitionsAssigned,
              onPartitionsLost,
            });

      runner = new Runner({
        logger: rootLogger,
        consumerGroup,
        instrumentationEmitter,
        heartbeatInterval,
        retry,
        autoCommit: runnerAutoCommit,
        eachBatchAutoResolve,
        eachBatch,
        eachMessage,
        onCrash,
        concurrency,
        prefetchMaxBatches,
        prefetchMaxBytes,
        hooks,
        maxRecords,
      });

      await runner.start();
    };

    const onCrash = async (e: Error): Promise<void> => {
      const error = e as Error & { retryCount?: number; retryTime?: number; host?: string; port?: number };
      logger.error(`Crash: ${error.name}: ${error.message}`, {
        groupId,
        retryCount: error.retryCount,
        stack: error.stack,
      });

      if (error.name === 'KafkaConnectionClosedError' && error.host != null && error.port != null) {
        cluster.removeBroker({ host: error.host, port: error.port });
      }

      await disconnect();

      const getOriginalCause = (cause: unknown): unknown => {
        if (cause && typeof cause === 'object' && 'cause' in cause && cause.cause) {
          return getOriginalCause(cause.cause);
        }
        return cause;
      };

      const originalCause = getOriginalCause(e);
      const isErrorRetriable =
        error.name === 'KafkaNumberOfRetriesExceeded' ||
        (typeof originalCause === 'object' &&
          originalCause != null &&
          'retriable' in originalCause &&
          originalCause.retriable === true);
      const shouldRestart =
        isErrorRetriable &&
        (!retry ||
          !retry.restartOnFailure ||
          (await retry.restartOnFailure(e).catch((callbackError: unknown) => {
            const cbError = callbackError as Error;
            logger.error(
              'Caught error when invoking user-provided "restartOnFailure" callback. Defaulting to restarting.',
              {
                error: cbError.message || cbError,
                cause: e.message || e,
                groupId,
              },
            );
            return true;
          })));

      instrumentationEmitter.emit(CRASH, {
        error: e,
        groupId,
        restart: shouldRestart,
      });

      if (shouldRestart) {
        const retryTime = error.retryTime || retry?.initialRetryTime || RETRY_DEFAULTS.initialRetryTime;
        logger.error(`Restarting the consumer in ${retryTime}ms`, {
          retryCount: error.retryCount,
          retryTime,
          groupId,
        });

        restartTimeout = setTimeout(() => {
          void start(onCrash);
        }, retryTime);
      }
    };

    if (signal) {
      signal.addEventListener(
        'abort',
        () => {
          void stop();
        },
        { once: true },
      );
    }

    await start(onCrash);
  };

  async function* stream(
    config: Omit<ConsumerRunConfig, 'eachBatch' | 'eachMessage'> = {},
  ): AsyncIterableIterator<Batch> {
    if (consumerGroup) {
      throw new KafkaNonRetriableError('Cannot stream while the consumer is already running');
    }

    type Pending = { batch: Batch; release: () => void };
    const pending: Pending[] = [];
    let notify: (() => void) | null = null;
    let finished = false;
    let failure: unknown;

    const wake = (): void => {
      notify?.();
      notify = null;
    };

    const wait = (): Promise<void> =>
      new Promise((resolve) => {
        notify = resolve;
      });

    const eachBatch: EachBatchHandler = async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      if (!isRunning() || isStale()) return;
      await heartbeat();

      await new Promise<void>((release) => {
        pending.push({ batch, release });
        wake();
      });

      resolveOffset(batch.lastOffset());
    };

    const removeStopListener = instrumentationEmitter.addListener(STOP, () => {
      finished = true;
      wake();
    });

    const runPromise = run({ ...config, eachBatch }).catch((error: unknown) => {
      failure = error;
      finished = true;
      wake();
    });

    try {
      while (true) {
        if (pending.length === 0 && !finished) {
          await wait();
        }

        if (failure) {
          throw failure instanceof Error ? failure : new Error('Consumer stream failed');
        }

        const item = pending.shift();
        if (!item) {
          if (finished) return;
          continue;
        }

        try {
          yield item.batch;
        } finally {
          item.release();
        }
      }
    } finally {
      wake();
      removeStopListener();
      await stop();
      await runPromise;
    }
  }

  const on = (
    eventName: ConsumerEventName,
    listener: (event: InstrumentationEvent<unknown>) => void | Promise<void>,
  ): RemoveInstrumentationEventListener => {
    if (!EVENT_NAMES.has(eventName)) {
      throw new KafkaNonRetriableError(`Event name should be one of ${EVENT_KEYS}`);
    }

    return instrumentationEmitter.addListener(unwrap(eventName), (event: InstrumentationEvent<unknown>) => {
      const wrapped = { ...event, type: wrap(event.type) };
      Promise.resolve(listener(wrapped)).catch((e: unknown) => {
        const error = e as Error;
        logger.error(`Failed to execute listener: ${error.message}`, { eventName, stack: error.stack });
      });
    });
  };

  const commitOffsets = async (topicPartitions: readonly TopicPartitionOffsetAndMetadata[] = []): Promise<void> => {
    const commitsByTopic = topicPartitions.reduce<
      Record<string, { partition: number; offset: bigint; metadata: string | null }[]>
    >((payload, { topic, partition, offset, metadata = null }) => {
      if (!topic) {
        throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
      }

      if (typeof partition !== 'number' || Number.isNaN(partition)) {
        throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${partition}`);
      }

      const commitOffset = parseOffset(offset);
      if (commitOffset < 0n) {
        throw new KafkaNonRetriableError('Offset must not be a negative number');
      }

      if (metadata !== null && typeof metadata !== 'string') {
        throw new KafkaNonRetriableError(
          `Invalid offset metadata, expected string or null, received ${String(metadata)}`,
        );
      }

      const topicCommits = payload[topic] ?? [];
      topicCommits.push({ partition, offset: commitOffset, metadata });
      payload[topic] = topicCommits;
      return payload;
    }, {});

    if (!consumerGroup || !runner) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    if (mode === 'assign' && !groupId) {
      throw new KafkaNonRetriableError(
        'Cannot commit offsets in assign() mode without a configured groupId. Pass groupId to kafka.consumer(...) to enable commitOffsets().',
      );
    }

    await runner.commitOffsets({
      topics: Object.keys(commitsByTopic).map((topic) => ({
        topic,
        partitions: commitsByTopic[topic] ?? [],
      })),
    });
  };

  const seek = ({
    topic,
    partition,
    offset,
  }: {
    topic: string;
    partition: number;
    offset: bigint | number | string;
  }): void => {
    if (!topic) {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    if (typeof partition !== 'number' || Number.isNaN(partition)) {
      throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${partition}`);
    }

    const seekOffset = parseOffset(offset);

    if (seekOffset < 0n && !SPECIAL_OFFSETS.has(seekOffset)) {
      throw new KafkaNonRetriableError('Offset must not be a negative number');
    }

    if (!consumerGroup) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    consumerGroup.seek({ topic, partition, offset: seekOffset });
  };

  const describeGroup = async (): Promise<GroupDescription> => {
    if (!groupId) {
      throw new KafkaNonRetriableError(
        'Consumer groupId must be configured to use describeGroup(). Pass groupId to kafka.consumer(...).',
      );
    }

    const coordinator = await cluster.findGroupCoordinator({ groupId });
    const describeRetrier = retrier(retry);
    return describeRetrier(async () => {
      const { groups } = await coordinator.describeGroups({ groupIds: [groupId] });
      const group = groups.find((entry) => entry.groupId === groupId);
      if (!group) {
        throw new KafkaNonRetriableError(`Consumer group ${groupId} not found`);
      }

      return {
        groupId: group.groupId ?? groupId,
        members: (group.members ?? []).map((member) => ({
          memberId: member.memberId ?? '',
          clientId: member.clientId ?? '',
          clientHost: member.clientHost ?? '',
          memberAssignment: member.memberAssignment ?? Buffer.alloc(0),
          memberMetadata: member.memberMetadata ?? Buffer.alloc(0),
        })),
        protocol: group.protocol ?? '',
        protocolType: group.protocolType ?? '',
        state: group.state ?? '',
      };
    });
  };

  const committed = async (
    topicPartitions: readonly TopicPartition[] = [],
  ): Promise<TopicPartitionOffsetAndMetadata[]> => {
    if (!groupId) {
      throw new KafkaNonRetriableError('Consumer groupId must be a non-empty string to fetch committed offsets');
    }

    if (topicPartitions.length === 0) {
      return [];
    }

    const partitionsByTopic = new Map<string, number[]>();
    for (const { topic, partition } of topicPartitions) {
      if (!topic) {
        throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
      }

      if (typeof partition !== 'number' || Number.isNaN(partition)) {
        throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${partition}`);
      }

      const partitions = partitionsByTopic.get(topic) ?? [];
      partitions.push(partition);
      partitionsByTopic.set(topic, partitions);
    }

    const coordinator = await cluster.findGroupCoordinator({ groupId });
    const committedRetrier = retrier(retry);
    return committedRetrier(async () => {
      const { responses } = await coordinator.offsetFetch({
        groupId,
        topics: [...partitionsByTopic.entries()].map(([topic, partitions]) => ({
          topic,
          partitions: partitions.map((partition) => ({ partition })),
        })),
      });

      const offsetsByKey = new Map<string, { offset: bigint; metadata: string | null }>();
      for (const { topic, partitions } of responses) {
        for (const { partition, offset, metadata } of partitions) {
          offsetsByKey.set(`${topic}:${partition}`, { offset, metadata: metadata ?? null });
        }
      }

      return topicPartitions.map(({ topic, partition }) => {
        const found = offsetsByKey.get(`${topic}:${partition}`);
        return {
          topic,
          partition,
          offset: found?.offset ?? BigInt(-1),
          metadata: found?.metadata ?? null,
        };
      });
    });
  };

  const position = ({ topic, partition }: TopicPartition): bigint | null => {
    if (!topic) {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    if (typeof partition !== 'number' || Number.isNaN(partition)) {
      throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${partition}`);
    }

    if (!consumerGroup) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    return consumerGroup.position(topic, partition);
  };

  const currentLag = ({ topic, partition }: TopicPartition): bigint | null => {
    if (!topic) {
      throw new KafkaNonRetriableError(`Invalid topic ${topic}`);
    }

    if (typeof partition !== 'number' || Number.isNaN(partition)) {
      throw new KafkaNonRetriableError(`Invalid partition, expected a number received ${partition}`);
    }

    if (!consumerGroup) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    const currentPosition = consumerGroup.position(topic, partition);
    const highWatermark = consumerGroup.highWatermark(topic, partition);
    if (currentPosition === null || highWatermark === null) {
      return null;
    }

    return highWatermark - currentPosition;
  };

  const pause = (topicPartitions: readonly { topic: string; partitions?: number[] }[] = []): void => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaNonRetriableError(`Invalid topic ${topicPartition?.topic ?? '<invalid>'}`);
      } else if (
        topicPartition.partitions !== undefined &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some((p) => Number.isNaN(p)))
      ) {
        throw new KafkaNonRetriableError(
          `Array of valid partitions required to pause specific partitions instead of ${String(topicPartition.partitions)}`,
        );
      }
    }

    if (!consumerGroup) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    consumerGroup.pause(topicPartitions);
  };

  const paused = (): TopicPartitions[] => {
    if (!consumerGroup) return [];
    return consumerGroup.paused();
  };

  const resume = (topicPartitions: readonly { topic: string; partitions?: number[] }[] = []): void => {
    for (const topicPartition of topicPartitions) {
      if (!topicPartition || !topicPartition.topic) {
        throw new KafkaNonRetriableError(`Invalid topic ${topicPartition?.topic ?? '<invalid>'}`);
      } else if (
        topicPartition.partitions !== undefined &&
        (!Array.isArray(topicPartition.partitions) || topicPartition.partitions.some((p) => Number.isNaN(p)))
      ) {
        throw new KafkaNonRetriableError(
          `Array of valid partitions required to resume specific partitions instead of ${String(topicPartition.partitions)}`,
        );
      }
    }

    if (!consumerGroup) {
      throw new KafkaNonRetriableError('Consumer group was not initialized, consumer#run must be called first');
    }

    consumerGroup.resume(topicPartitions);
  };

  return {
    connect,
    disconnect,
    subscribe,
    assign,
    stop,
    run,
    stream,
    commitOffsets,
    seek,
    describeGroup,
    committed,
    position,
    currentLag,
    pause,
    paused,
    resume,
    on,
    events,
    logger: () => logger,
    [Symbol.asyncDispose]: disconnect,
  };
}
