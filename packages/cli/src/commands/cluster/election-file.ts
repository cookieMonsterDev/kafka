import { CliUsageError } from '../../args/coerce';

// Not exported from `@cookiemonsterdev/kafka-core`'s public surface — mirrors
// `Admin.electLeaders`'s own inline `TopicPartitions` shape.
export interface TopicPartitions {
  readonly topic: string;
  readonly partitions: number[];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Validates and normalizes the `--path-to-json-file` shape `kafka-leader-election.sh` uses:
 * `{"partitions":[{"topic":"foo","partition":1},{"topic":"foobar","partition":2}]}` — no
 * top-level `version` field, unlike the reassignment file.
 */
export function parseElectionFile(raw: unknown, flagName = 'from-file'): TopicPartitions[] {
  if (!isPlainObject(raw) || !Array.isArray(raw.partitions)) {
    throw new CliUsageError(`--${flagName} must be a JSON object with a "partitions" array`);
  }
  if (raw.partitions.length === 0) {
    throw new CliUsageError(`--${flagName} lists no partitions`);
  }

  const byTopic = new Map<string, number[]>();

  raw.partitions.forEach((entry: unknown, index: number) => {
    if (!isPlainObject(entry) || typeof entry.topic !== 'string' || entry.topic.length === 0) {
      throw new CliUsageError(`--${flagName} "partitions[${index}]" must have a non-empty string "topic"`);
    }
    if (!Number.isInteger(entry.partition) || (entry.partition as number) < 0) {
      throw new CliUsageError(`--${flagName} "partitions[${index}]" must have a non-negative integer "partition"`);
    }

    const partitions = byTopic.get(entry.topic) ?? [];
    if (!partitions.includes(entry.partition as number)) partitions.push(entry.partition as number);
    byTopic.set(entry.topic, partitions);
  });

  return [...byTopic.entries()].map(([topic, partitions]) => ({ topic, partitions }));
}
