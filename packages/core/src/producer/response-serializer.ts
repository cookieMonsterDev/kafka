import type { ProduceResponseV6Body } from '../protocol/requests/produce/v6/response';
import type { RecordMetadata } from './types';

export function responseSerializer(response: ProduceResponseV6Body): RecordMetadata[] {
  return response.topics.flatMap(({ topicName, partitions }) =>
    partitions.map((partition) => ({ topicName, ...partition })),
  );
}
