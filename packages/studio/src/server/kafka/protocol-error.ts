const MAX_CAUSE_DEPTH = 10;

/**
 * A broker protocol error's `.type` (e.g. `"UNKNOWN_TOPIC_OR_PARTITION"`), found by walking either
 * of the two shapes a retriable-or-batched call re-throws as: a retriable error that exhausts its
 * retries wraps the original as `.cause` (its own `.type` is `undefined`); a batched call that
 * fans out to several resources (e.g. `describeTopicPartitions`) throws a `KafkaAggregateError`
 * whose per-resource failures live in `.errors` instead. Checked in that order at each level, so a
 * mix of both shapes at different depths still resolves. Matched by field, never `instanceof`: a
 * config file's core may not be the same installed copy as this package's.
 */
export function protocolErrorType(error: unknown, depth = 0): string | undefined {
  if (depth > MAX_CAUSE_DEPTH || typeof error !== 'object' || error === null) return undefined;
  const type = (error as { type?: unknown }).type;
  if (typeof type === 'string') return type;

  const cause = protocolErrorType((error as { cause?: unknown }).cause, depth + 1);
  if (cause !== undefined) return cause;

  const errors = (error as { errors?: unknown }).errors;
  if (Array.isArray(errors)) {
    for (const sub of errors) {
      const found = protocolErrorType(sub, depth + 1);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

export function isUnknownTopicOrPartitionError(error: unknown): boolean {
  return protocolErrorType(error) === 'UNKNOWN_TOPIC_OR_PARTITION';
}

export function isTopicAlreadyExistsError(error: unknown): boolean {
  return protocolErrorType(error) === 'TOPIC_ALREADY_EXISTS';
}

export function isGroupIdNotFoundError(error: unknown): boolean {
  return protocolErrorType(error) === 'GROUP_ID_NOT_FOUND';
}

export function isNonEmptyGroupError(error: unknown): boolean {
  return protocolErrorType(error) === 'NON_EMPTY_GROUP';
}

/** The broker has no `authorizer.class.name` configured — the default for most non-production clusters, this one's own `docker-compose.dev.yml` included. */
export function isSecurityDisabledError(error: unknown): boolean {
  return protocolErrorType(error) === 'SECURITY_DISABLED';
}
