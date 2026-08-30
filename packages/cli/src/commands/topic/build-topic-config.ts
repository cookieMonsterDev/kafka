import type { TopicConfig } from '@cookiemonsterdev/kafka-core';
import { CliUsageError, splitKeyValue } from '../../args/coerce';

interface ReplicaAssignment {
  readonly partition: number;
  readonly replicas: number[];
}

function parseAssignmentInt(raw: string, what: string): number {
  const value = Number(raw);
  if (!Number.isInteger(value)) {
    throw new CliUsageError(`--replica-assignment ${what} must be an integer, got "${raw}"`);
  }
  return value;
}

/** Decodes repeated `partition=replica,replica` entries into `ReplicaAssignment[]`. */
export function parseReplicaAssignment(raw: readonly string[]): ReplicaAssignment[] {
  return raw.map((entry) => {
    const [partitionRaw, replicasRaw] = splitKeyValue(entry, 'replica-assignment');
    const partition = parseAssignmentInt(partitionRaw, 'partition');
    const replicas = replicasRaw
      .split(',')
      .map((replica) => replica.trim())
      .filter((replica) => replica.length > 0)
      .map((replica) => parseAssignmentInt(replica, 'replica id'));
    if (replicas.length === 0) {
      throw new CliUsageError(`--replica-assignment "${entry}" lists no replicas`);
    }
    return { partition, replicas };
  });
}

export interface TopicCreateFlags {
  readonly partitions?: number;
  readonly replicationFactor?: number;
  readonly replicaAssignment?: readonly string[];
  readonly config?: Readonly<Record<string, string>>;
}

/** Builds one `TopicConfig` from a topic name and the command's own flags — no I/O, fully pure. */
export function buildTopicConfig(topic: string, flags: TopicCreateFlags): TopicConfig {
  const hasCount = flags.partitions !== undefined || flags.replicationFactor !== undefined;
  const hasAssignment = flags.replicaAssignment !== undefined && flags.replicaAssignment.length > 0;
  if (hasCount && hasAssignment) {
    throw new CliUsageError('--partitions/--replication-factor cannot be combined with --replica-assignment');
  }

  const configEntries =
    flags.config !== undefined ? Object.entries(flags.config).map(([name, value]) => ({ name, value })) : undefined;

  if (hasAssignment) {
    return {
      topic,
      replicaAssignment: parseReplicaAssignment(flags.replicaAssignment ?? []),
      ...(configEntries !== undefined ? { configEntries } : {}),
    };
  }

  return {
    topic,
    ...(flags.partitions !== undefined ? { numPartitions: flags.partitions } : {}),
    ...(flags.replicationFactor !== undefined ? { replicationFactor: flags.replicationFactor } : {}),
    ...(configEntries !== undefined ? { configEntries } : {}),
  };
}
