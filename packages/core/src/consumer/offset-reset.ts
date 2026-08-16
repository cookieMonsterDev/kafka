/**
 * Java `auto.offset.reset` policy.
 * @see https://kafka.apache.org/43/configuration/consumer-configs/#auto.offset.reset
 */
export type AutoOffsetReset = 'earliest' | 'latest' | 'none';

/** Per-topic offset reset options stored after subscribe. */
export interface TopicOffsetConfiguration {
  fromBeginning?: boolean;
  autoOffsetReset?: AutoOffsetReset;
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
