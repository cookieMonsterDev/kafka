import type { Offsets, OffsetsByTopicPartition, TopicPartition, TopicPartitionOffset } from '../types';

/**
 * Shape shared by `OffsetManager` (subscribe/group mode) and `AssignedOffsetManager`
 * (`assign()` mode). `ConsumerGroup` depends on this interface rather than a concrete class, so
 * it can swap implementations based on how the consumer was started, while reusing the same
 * fetch loop, pause/resume, and seek machinery either way.
 */
export interface OffsetManagerHandle {
  nextOffset: (topic: string, partition: number) => bigint;
  resetOffset: (topicPartition: TopicPartition) => void;
  resolveOffset: (topicPartitionOffset: TopicPartitionOffset) => void;
  countResolvedOffsets: () => bigint;
  setDefaultOffset: (topicPartition: TopicPartition) => Promise<void>;
  seek: (topicPartitionOffset: TopicPartitionOffset) => Promise<void>;
  commitOffsetsIfNecessary: () => Promise<void>;
  uncommittedOffsets: () => OffsetsByTopicPartition;
  commitOffsets: (offsets?: Offsets) => Promise<void>;
  resolveOffsets: () => Promise<void>;
  clearOffsets: (topicPartition: TopicPartition) => void;
  clearAllOffsets: () => void;
  committedOffsets: () => Record<string, Record<string, bigint>>;
}
