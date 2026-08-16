import { array, defineRequest, field, int32, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * OffsetForLeaderEpoch Request (Version: 0) => [topics]
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition leader_epoch
 *       partition => INT32
 *       leader_epoch => INT32
 */
const partitionSchema = object([field('partition', int32), field('leaderEpoch', int32)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([field('topics', array(topicSchema))]);

export const offsetForLeaderEpochRequestV0 = defineRequest({
  apiKey: API_KEYS.OffsetForLeaderEpoch,
  apiVersion: 0,
  apiName: 'OffsetForLeaderEpoch',
  schema: requestSchema,
});
