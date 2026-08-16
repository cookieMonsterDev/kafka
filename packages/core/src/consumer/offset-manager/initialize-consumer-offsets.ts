import { KafkaNonRetriableError } from '../../errors';
import type { AutoOffsetReset } from '../offset-reset';
import { isInvalidOffset } from './is-invalid-offset';

interface OffsetPartition {
  partition: number;
  offset: bigint;
}

interface OffsetTopic {
  topic: string;
  partitions: readonly OffsetPartition[];
}

function indexPartitions(obj: Record<string, bigint>, { partition, offset }: OffsetPartition): Record<string, bigint> {
  return { ...obj, [partition]: offset };
}

function indexTopics(
  obj: Record<string, Record<string, bigint>>,
  { topic, partitions }: OffsetTopic,
): Record<string, Record<string, bigint>> {
  return { ...obj, [topic]: partitions.reduce(indexPartitions, {}) };
}

/**
 * For each committed consumer offset that is still invalid (`-1` = "never committed"), substitute
 * the matching topic offset from `ListOffsets` (earliest/latest depending on `fromBeginning`).
 * When the reset policy is `none`, throw instead of substituting.
 */
export function initializeConsumerOffsets(
  consumerOffsets: readonly OffsetTopic[],
  topicOffsets: readonly OffsetTopic[],
  resetByTopic: Readonly<Record<string, AutoOffsetReset>> = {},
): { topic: string; partitions: { partition: number; offset: bigint }[] }[] {
  const indexedConsumerOffsets = consumerOffsets.reduce(indexTopics, {});
  const indexedTopicOffsets = topicOffsets.reduce(indexTopics, {});

  return Object.keys(indexedConsumerOffsets).map((topic) => {
    const partitions = indexedConsumerOffsets[topic] ?? {};
    const topicOffsetsForTopic = indexedTopicOffsets[topic] ?? {};
    const reset = resetByTopic[topic];

    return {
      topic,
      partitions: Object.keys(partitions).map((partition) => {
        const offset = partitions[partition];
        if (!isInvalidOffset(offset)) {
          return { partition: Number(partition), offset: offset ?? 0n };
        }

        if (reset === 'none') {
          throw new KafkaNonRetriableError(
            `Offset reset policy is none; no committed offset for topic ${topic} partition ${partition}`,
          );
        }

        const resolvedOffset = topicOffsetsForTopic[partition];
        return { partition: Number(partition), offset: resolvedOffset ?? 0n };
      }),
    };
  });
}
