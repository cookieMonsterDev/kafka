import { CliUsageError } from '../args/coerce';

/** Every connecting command takes `--brokers` as a comma-separated host:port list. */
export function parseBrokersFlag(raw: unknown): string[] {
  if (typeof raw !== 'string') {
    throw new CliUsageError('--brokers is required (a comma-separated host:port list, e.g. localhost:9092)');
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
