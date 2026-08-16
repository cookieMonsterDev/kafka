import type { PartitionMetadata } from '../cluster/index.js';
import type { CustomPartitioner, Message } from './types.js';

export interface GroupMessagesPerPartitionOptions {
  topic: string;
  partitionMetadata: readonly PartitionMetadata[];
  messages: readonly Message[];
  partitioner: ReturnType<CustomPartitioner>;
}

export function groupMessagesPerPartition({
  topic,
  partitionMetadata,
  messages,
  partitioner,
}: GroupMessagesPerPartitionOptions): Map<number, Message[]> {
  const messagesPerPartition = new Map<number, Message[]>();
  if (partitionMetadata.length === 0) return messagesPerPartition;

  for (const message of messages) {
    const partition = partitioner({ topic, partitionMetadata, message });
    const current = messagesPerPartition.get(partition);
    if (current) {
      current.push(message);
    } else {
      messagesPerPartition.set(partition, [message]);
    }
  }

  return messagesPerPartition;
}
