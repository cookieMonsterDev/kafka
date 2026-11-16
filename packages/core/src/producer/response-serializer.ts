import type { ProduceResponseV6Body } from '../protocol/requests/produce/v6/response';
import type { RecordMetadata } from './types';

export function responseSerializer(response: ProduceResponseV6Body): RecordMetadata[] {
  const recordMetadata: RecordMetadata[] = [];
  for (const { topicName, partitions } of response.topics) {
    for (const partition of partitions) recordMetadata.push({ topicName, ...partition });
  }
  return recordMetadata;
}
