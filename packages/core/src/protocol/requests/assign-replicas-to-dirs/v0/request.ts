import { compactArray, defineRequest, field, flexibleObject, int32, int64, uuid } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AssignReplicasToDirsPartition {
  partitionIndex: number;
}

export interface AssignReplicasToDirsTopic {
  topicId: Buffer;
  partitions: AssignReplicasToDirsPartition[];
}

export interface AssignReplicasToDirsDirectory {
  id: Buffer;
  topics: AssignReplicasToDirsTopic[];
}

export interface AssignReplicasToDirsRequestV0Fields {
  brokerId: number;
  brokerEpoch: bigint;
  directories: AssignReplicasToDirsDirectory[];
}

/**
 * AssignReplicasToDirs Request (Version: 0) => broker_id broker_epoch [directories] TAG_BUFFER
 *   broker_id => INT32
 *   broker_epoch => INT64
 *   directories => id [topics] TAG_BUFFER
 *     id => UUID
 *     topics => topic_id [partitions] TAG_BUFFER
 *       topic_id => UUID
 *       partitions => partition_index TAG_BUFFER
 *         partition_index => INT32
 *
 * Flexible from v0. Controller-only (KIP-858).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32)]);
const topicSchema = flexibleObject([field('topicId', uuid), field('partitions', compactArray(partitionSchema))]);
const directorySchema = flexibleObject([field('id', uuid), field('topics', compactArray(topicSchema))]);
export const requestSchema = flexibleObject([
  field('brokerId', int32),
  field('brokerEpoch', int64),
  field('directories', compactArray(directorySchema)),
]);

export const assignReplicasToDirsRequestV0 = defineRequest({
  apiKey: API_KEYS.AssignReplicasToDirs,
  apiVersion: 0,
  apiName: 'AssignReplicasToDirs',
  schema: requestSchema,
});
