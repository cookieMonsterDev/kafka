import type { ProduceResponse } from '../protocol/requests/produce/index';
import type { RecordMetadata } from './types';

export function responseSerializer(response: NonNullable<ProduceResponse>): RecordMetadata[] {
  const recordMetadata: RecordMetadata[] = [];
  for (const { topicName, partitions } of response.topics) {
    for (const partition of partitions) recordMetadata.push({ topicName, ...partition });
  }
  return recordMetadata;
}
