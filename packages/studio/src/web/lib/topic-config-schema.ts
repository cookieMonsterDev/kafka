type TopicConfigKind = 'boolean' | 'enum' | 'non-negative-int' | 'ratio';

interface TopicConfigSpec {
  readonly kind: TopicConfigKind;
  readonly enumValues?: readonly string[];
  /** `-1` is Kafka's own sentinel for "unlimited" on a handful of size/duration configs. */
  readonly allowsUnlimited?: boolean;
}

/**
 * The well-known, broker-documented topic-level configs (`kafka-topics.sh`'s own config set) —
 * not exhaustive of every plugin-defined key a cluster might expose, but covers what an operator
 * edits day to day. A key outside this map is still editable; it just isn't validated client-side,
 * since the broker is the only real authority on a config it defines itself.
 */
const KNOWN_TOPIC_CONFIGS: Readonly<Record<string, TopicConfigSpec>> = {
  'cleanup.policy': { kind: 'enum', enumValues: ['delete', 'compact', 'compact,delete'] },
  'compression.type': { kind: 'enum', enumValues: ['uncompressed', 'zstd', 'lz4', 'snappy', 'gzip', 'producer'] },
  'delete.retention.ms': { kind: 'non-negative-int' },
  'file.delete.delay.ms': { kind: 'non-negative-int' },
  'flush.messages': { kind: 'non-negative-int' },
  'flush.ms': { kind: 'non-negative-int' },
  'index.interval.bytes': { kind: 'non-negative-int' },
  'local.retention.bytes': { kind: 'non-negative-int', allowsUnlimited: true },
  'local.retention.ms': { kind: 'non-negative-int', allowsUnlimited: true },
  'max.compaction.lag.ms': { kind: 'non-negative-int' },
  'max.message.bytes': { kind: 'non-negative-int' },
  'message.downconversion.enable': { kind: 'boolean' },
  'message.timestamp.type': { kind: 'enum', enumValues: ['CreateTime', 'LogAppendTime'] },
  'min.cleanable.dirty.ratio': { kind: 'ratio' },
  'min.compaction.lag.ms': { kind: 'non-negative-int' },
  'min.insync.replicas': { kind: 'non-negative-int' },
  preallocate: { kind: 'boolean' },
  'remote.storage.enable': { kind: 'boolean' },
  'retention.bytes': { kind: 'non-negative-int', allowsUnlimited: true },
  'retention.ms': { kind: 'non-negative-int', allowsUnlimited: true },
  'segment.bytes': { kind: 'non-negative-int' },
  'segment.index.bytes': { kind: 'non-negative-int' },
  'segment.jitter.ms': { kind: 'non-negative-int' },
  'segment.ms': { kind: 'non-negative-int' },
  'unclean.leader.election.enable': { kind: 'boolean' },
};

/** `null` means valid (or the key isn't one this module has an opinion on) — otherwise the message to show. */
export function validateTopicConfigValue(name: string, value: string): string | null {
  const spec = KNOWN_TOPIC_CONFIGS[name];
  if (spec === undefined) return null;

  const trimmed = value.trim();
  if (trimmed === '') return 'value is required';

  switch (spec.kind) {
    case 'boolean':
      return trimmed === 'true' || trimmed === 'false' ? null : 'must be "true" or "false"';
    case 'enum':
      return spec.enumValues?.includes(trimmed) ? null : `must be one of: ${spec.enumValues?.join(', ') ?? ''}`;
    case 'non-negative-int':
      if (spec.allowsUnlimited && trimmed === '-1') return null;
      return /^\d+$/.test(trimmed) ? null : 'must be a non-negative whole number';
    case 'ratio': {
      const numeric = Number(trimmed);
      return Number.isFinite(numeric) && numeric >= 0 && numeric <= 1 ? null : 'must be a number between 0 and 1';
    }
  }
}
