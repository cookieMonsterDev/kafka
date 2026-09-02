import { CliUsageError } from '../../args/coerce';

// Not exported from `@cookiemonsterdev/kafka-core`'s public surface — mirrors
// `Admin.alterPartitionReassignments`'s own inline `PartitionReassignment` shape.
export interface ReplicaAssignment {
  readonly partition: number;
  readonly replicas: number[];
}

export interface PartitionReassignment {
  readonly topic: string;
  readonly partitionAssignment: ReplicaAssignment[];
}

export interface ParsedReassignmentFile {
  readonly topics: PartitionReassignment[];
  /** Whether any entry carried `log_dirs` — `cluster reassign execute` does not apply these. */
  readonly hasLogDirs: boolean;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Validates and normalizes the `--reassignment-json-file` shape `kafka-reassign-partitions.sh`
 * uses for `--execute`/`--verify`/`--cancel`:
 * `{"partitions":[{"topic":"foo","partition":1,"replicas":[1,2,3],"log_dirs":["dir1","dir2","dir3"]}],"version":1}`.
 * `version` is accepted but not validated — the same choice `topic/delete-records.ts` already
 * made for the sibling `--offset-json-file` shape. `log_dirs`, when present, must be either
 * `"any"` or an absolute path per partition, one per replica — but nothing in this command applies
 * it (see `ParsedReassignmentFile.hasLogDirs`): moving a replica between log dirs on the same
 * broker is a separate `AlterReplicaLogDirs` call, and that API stays passthrough-only for now.
 */
export function parseReassignmentFile(raw: unknown, flagName = 'from-file'): ParsedReassignmentFile {
  if (!isPlainObject(raw) || !Array.isArray(raw.partitions)) {
    throw new CliUsageError(`--${flagName} must be a JSON object with a "partitions" array`);
  }
  if (raw.partitions.length === 0) {
    throw new CliUsageError(`--${flagName} lists no partitions`);
  }

  const byTopic = new Map<string, ReplicaAssignment[]>();
  let hasLogDirs = false;

  raw.partitions.forEach((entry: unknown, index: number) => {
    if (!isPlainObject(entry) || typeof entry.topic !== 'string' || entry.topic.length === 0) {
      throw new CliUsageError(`--${flagName} "partitions[${index}]" must have a non-empty string "topic"`);
    }
    if (!Number.isInteger(entry.partition) || (entry.partition as number) < 0) {
      throw new CliUsageError(`--${flagName} "partitions[${index}]" must have a non-negative integer "partition"`);
    }
    if (!Array.isArray(entry.replicas) || entry.replicas.length === 0) {
      throw new CliUsageError(`--${flagName} "partitions[${index}]" must have a non-empty "replicas" array`);
    }
    if (entry.replicas.some((replica) => !Number.isInteger(replica) || (replica as number) < 0)) {
      throw new CliUsageError(`--${flagName} "partitions[${index}].replicas" must contain only non-negative integers`);
    }

    if (entry.log_dirs !== undefined) {
      if (!Array.isArray(entry.log_dirs) || entry.log_dirs.length !== entry.replicas.length) {
        throw new CliUsageError(
          `--${flagName} "partitions[${index}].log_dirs", when present, must have exactly one entry per replica`,
        );
      }
      if (entry.log_dirs.some((dir) => typeof dir !== 'string' || (dir !== 'any' && !dir.startsWith('/')))) {
        throw new CliUsageError(
          `--${flagName} "partitions[${index}].log_dirs" entries must each be "any" or an absolute path`,
        );
      }
      hasLogDirs = true;
    }

    const partitionAssignment = byTopic.get(entry.topic) ?? [];
    partitionAssignment.push({ partition: entry.partition as number, replicas: entry.replicas as number[] });
    byTopic.set(entry.topic, partitionAssignment);
  });

  const topics = [...byTopic.entries()].map(([topic, partitionAssignment]) => ({ topic, partitionAssignment }));
  return { topics, hasLogDirs };
}
