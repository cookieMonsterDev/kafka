import { KafkaNonRetriableError } from '../errors';

/**
 * Offset reset policy (`auto.offset.reset`). `by_duration:` takes an ISO-8601 duration
 * (KIP-1106); the client resolves it to a ListOffsets timestamp.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
 */
export type AutoOffsetReset = 'earliest' | 'latest' | 'none' | `by_duration:${string}`;

export const BY_DURATION_PREFIX = 'by_duration:';

/** Per-topic offset reset options stored after subscribe. */
export interface TopicOffsetConfiguration {
  fromBeginning?: boolean;
  autoOffsetReset?: AutoOffsetReset;
}

/** Shape passed to `cluster.fetchTopicsOffset` for one unresolved topic. */
export interface ListOffsetsResetQuery {
  topic: string;
  partitions: readonly { partition: number }[];
  fromBeginning?: boolean;
  fromTimestamp?: bigint;
}

/**
 * Resolve the offset reset policy. `autoOffsetReset` wins when set;
 * otherwise `fromBeginning === true` maps to earliest and everything else to latest.
 */
export function resolveAutoOffsetReset(config: TopicOffsetConfiguration | undefined): AutoOffsetReset {
  if (config?.autoOffsetReset != null) {
    return config.autoOffsetReset;
  }

  return config?.fromBeginning === true ? 'earliest' : 'latest';
}

/** Build the stored topic configuration from subscribe options and an optional consumer default. */
export function topicOffsetConfigurationFromSubscribe(
  subscription: TopicOffsetConfiguration,
  defaultAutoOffsetReset?: AutoOffsetReset,
): TopicOffsetConfiguration {
  const fromBeginning = subscription.fromBeginning ?? false;
  const autoOffsetReset = subscription.autoOffsetReset ?? defaultAutoOffsetReset;
  if (autoOffsetReset == null) {
    return { fromBeginning };
  }

  return { fromBeginning, autoOffsetReset };
}

export function isByDurationReset(reset: string): reset is `by_duration:${string}` {
  return reset.startsWith(BY_DURATION_PREFIX);
}

/**
 * Parse an ISO-8601 duration the same way Kafka's `by_duration:` form does (`PnDTnHnMn.nS`).
 * Years and months are rejected (they are calendar periods, not a fixed millisecond length).
 */
export function parseIso8601DurationMs(value: string): number {
  const match = /^(-)?P(?!$)(?:(\d+)D)?(?:T(?!$)(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(value);
  if (!match) {
    throw new KafkaNonRetriableError(
      `Invalid ISO-8601 duration "${value}". Use PnDTnHnMn.nS (for example PT1H, P2D, PT30M).`,
    );
  }

  const [, negative, days, hours, minutes, seconds] = match;
  const totalMs =
    Number(days ?? 0) * 86_400_000 +
    Number(hours ?? 0) * 3_600_000 +
    Number(minutes ?? 0) * 60_000 +
    Number(seconds ?? 0) * 1_000;

  return negative ? -totalMs : totalMs;
}

/** ListOffsets timestamp for a `by_duration:` reset: `now - duration`. */
export function timestampForByDurationReset(reset: AutoOffsetReset, nowMs = Date.now()): bigint {
  if (!isByDurationReset(reset)) {
    throw new KafkaNonRetriableError(`Expected a by_duration reset, received "${reset}"`);
  }

  const spec = reset.slice(BY_DURATION_PREFIX.length);
  return BigInt(nowMs) - BigInt(parseIso8601DurationMs(spec));
}

/**
 * Convert a resolved reset policy into `fetchTopicsOffset` arguments. Returns `null` for `none`
 * (caller should throw instead of listing offsets).
 */
export function listOffsetsQueryForReset(
  topic: string,
  partitions: readonly { partition: number }[],
  reset: AutoOffsetReset,
  nowMs = Date.now(),
): ListOffsetsResetQuery | null {
  if (reset === 'none') return null;
  if (isByDurationReset(reset)) {
    return { topic, partitions: [...partitions], fromTimestamp: timestampForByDurationReset(reset, nowMs) };
  }
  return { topic, partitions: [...partitions], fromBeginning: reset === 'earliest' };
}
