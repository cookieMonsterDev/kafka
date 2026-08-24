import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetForLeaderEpoch Request (Version: 3) => replica_id [topics]
 *   replica_id => INT32
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch leader_epoch
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       leader_epoch => INT32
 *
 * `replica_id` is new at this version. Still classic (non-flexible) encoding - KIP-482 compact
 * types and tagged fields don't apply to OffsetForLeaderEpoch until v4.
 */
const partitionSchema = object([
  field('partition', int32),
  field('currentLeaderEpoch', int32),
  field('leaderEpoch', int32),
]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('replicaId', int32), field('topics', array(topicSchema))]);

export const offsetForLeaderEpochRequestV3 = defineRequest({
  apiKey: API_KEYS.OffsetForLeaderEpoch,
  apiVersion: 3,
  apiName: 'OffsetForLeaderEpoch',
  schema: requestSchema,
});
