const MAX_CAUSE_DEPTH = 10;

/**
 * A broker protocol error's `.type` (e.g. `"UNKNOWN_TOPIC_OR_PARTITION"`), walking `.cause` to
 * find it. Needed because a retriable protocol error that exhausts its retries is re-thrown as
 * `KafkaNumberOfRetriesExceeded`, which carries the original error only as `.cause` — its own
 * `.type` is `undefined`. Matched by field, never `instanceof`, same as every other error check
 * in this package (a config file's core may not be the same installed copy as the CLI's own).
 */
export function protocolErrorType(error: unknown, depth = 0): string | undefined {
  if (depth > MAX_CAUSE_DEPTH || typeof error !== 'object' || error === null) return undefined;
  const type = (error as { type?: unknown }).type;
  if (typeof type === 'string') return type;
  return protocolErrorType((error as { cause?: unknown }).cause, depth + 1);
}

export function isUnknownTopicOrPartitionError(error: unknown): boolean {
  return protocolErrorType(error) === 'UNKNOWN_TOPIC_OR_PARTITION';
}
