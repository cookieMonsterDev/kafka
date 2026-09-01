import { CliUsageError } from '../args/coerce';

/**
 * Every connecting command takes an optional `--brokers` — a comma-separated host:port list.
 * Omitting it is no longer a usage error: a config file, an environment variable, or a
 * `--profile` may resolve it instead. If nothing resolves one at all, core's own constructor
 * raises `MissingBrokers` once a connection is actually attempted.
 */
export function parseBrokersFlag(raw: unknown): string[] | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'string') {
    throw new CliUsageError('--brokers must be a comma-separated host:port list, e.g. localhost:9092');
  }
  const brokers = raw
    .split(',')
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);
  if (brokers.length === 0) {
    throw new CliUsageError('--brokers must list at least one broker');
  }
  return brokers;
}
