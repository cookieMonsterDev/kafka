import { compactArray, compactNullableArray, compactString, field, flexibleObject, int32, uuid } from '../../schema';

export interface ShareGroupHeartbeatTopicPartitions {
  topicId: Buffer;
  partitions: number[];
}

export const heartbeatTopicPartitionsSchema = flexibleObject([
  field('topicId', uuid),
  field('partitions', compactArray(int32)),
]);

export const nullableHeartbeatTopicPartitions = compactNullableArray(heartbeatTopicPartitionsSchema);

export const compactTopicNames = compactNullableArray(compactString);
