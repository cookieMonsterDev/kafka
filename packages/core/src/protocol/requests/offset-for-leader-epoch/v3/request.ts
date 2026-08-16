import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetForLeaderEpoch Request (Version: 3) => replica_id [topics] TAG_BUFFER
 *   replica_id => INT32
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition current_leader_epoch leader_epoch TAG_BUFFER
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       leader_epoch => INT32
 *
 * Flexible (KIP-482): compact types and a trailing TAG_BUFFER on every struct, including the root.
 */
const partitionSchema = flexibleObject([
  field('partition', int32),
  field('currentLeaderEpoch', int32),
  field('leaderEpoch', int32),
]);
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);
const requestSchema = flexibleObject([field('replicaId', int32), field('topics', compactArray(topicSchema))]);

export const offsetForLeaderEpochRequestV3 = defineRequest({
  apiKey: API_KEYS.OffsetForLeaderEpoch,
  apiVersion: 3,
  apiName: 'OffsetForLeaderEpoch',
  schema: requestSchema,
});
