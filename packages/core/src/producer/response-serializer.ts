import type { ProduceResponse } from '../protocol/requests/produce/index';
import type { RecordMetadata } from './types';

export function responseSerializer(response: NonNullable<ProduceResponse>): RecordMetadata[] {
  const recordMetadata: RecordMetadata[] = [];
  for (const { topicName, partitions } of response.topics) {
    // Produce v3 and v4 partitions carry no `logStartOffset`, which `RecordMetadata` types as present.
    for (const partition of partitions) recordMetadata.push({ topicName, ...partition } as RecordMetadata);
  }
  return recordMetadata;
}
