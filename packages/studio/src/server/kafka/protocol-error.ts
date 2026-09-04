const MAX_CAUSE_DEPTH = 10;

/**
 * A broker protocol error's `.type` (e.g. `"UNKNOWN_TOPIC_OR_PARTITION"`), walking `.cause` to find
 * it — a retriable protocol error that exhausts its retries is re-thrown as a wrapper whose own
 * `.type` is `undefined` and carries the original error only as `.cause`. Matched by field, never
 * `instanceof`: a config file's core may not be the same installed copy as this package's.
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

export function isTopicAlreadyExistsError(error: unknown): boolean {
  return protocolErrorType(error) === 'TOPIC_ALREADY_EXISTS';
}
