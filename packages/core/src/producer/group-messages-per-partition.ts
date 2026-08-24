import type { PartitionMetadata } from '../cluster/index';
import type { NodeLatencyReader } from './node-latency-tracker';
import type { CustomPartitioner, Message } from './types';

export interface GroupMessagesPerPartitionOptions {
  topic: string;
  partitionMetadata: readonly PartitionMetadata[];
  messages: readonly Message[];
  partitioner: ReturnType<CustomPartitioner>;
  nodeLatency?: NodeLatencyReader;
}

export function groupMessagesPerPartition({
  topic,
  partitionMetadata,
  messages,
  partitioner,
  nodeLatency,
}: GroupMessagesPerPartitionOptions): Map<number, Message[]> {
  const messagesPerPartition = new Map<number, Message[]>();
  if (partitionMetadata.length === 0) return messagesPerPartition;

  partitioner.onNewBatch?.({ topic, partitionMetadata, nodeLatency });

  for (const message of messages) {
    const partition = partitioner({ topic, partitionMetadata, message, nodeLatency });
    const current = messagesPerPartition.get(partition);
    if (current) {
      current.push(message);
    } else {
      messagesPerPartition.set(partition, [message]);
    }
  }

  return messagesPerPartition;
}
