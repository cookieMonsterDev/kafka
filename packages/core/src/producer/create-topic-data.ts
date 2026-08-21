import type { ProduceMessage, ProducePartitionData, ProduceTopicData } from '../protocol/requests/produce/shared';
import type { Message } from './types';

export interface TopicDataInput {
  topic: string;
  topicId?: Buffer;
  partitions: readonly number[];
  messagesPerPartition: ReadonlyMap<number, readonly Message[]>;
}

function toProduceMessage(message: Message): ProduceMessage {
  return { key: message.key, value: message.value, timestamp: message.timestamp, headers: message.headers };
}

/** Shapes grouped-by-partition messages into the wire-ready `topicData` the Produce request expects. */
export function createTopicData(topicDataForBroker: readonly TopicDataInput[]): ProduceTopicData[] {
  return topicDataForBroker.map(({ topic, topicId, partitions, messagesPerPartition }) => ({
    topic,
    ...(topicId != null ? { topicId } : {}),
    partitions: partitions.map((partition): ProducePartitionData => ({
      partition,
      messages: (messagesPerPartition.get(partition) ?? []).map(toProduceMessage),
    })),
  }));
}
