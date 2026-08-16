import { array, defineRequest, field, int8, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListOffsets Request (Version: 4) => replica_id isolation_level [topics]
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch timestamp
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       timestamp => INT64
 *
 * KIP-320: `current_leader_epoch` lets the broker reject stale leaders. Unknown epoch is -1.
 */
export const listOffsetsRequestSchemaV4 = object([
  field('replicaId', int32),
  field('isolationLevel', int8),
  field(
    'topics',
    array(
      object([
        field('topic', string),
        field(
          'partitions',
          array(object([field('partition', int32), field('currentLeaderEpoch', int32), field('timestamp', int64)])),
        ),
      ]),
    ),
  ),
]);

export const listOffsetsRequestV4 = defineRequest({
  apiKey: API_KEYS.ListOffsets,
  apiVersion: 4,
  apiName: 'ListOffsets',
  schema: listOffsetsRequestSchemaV4,
});
