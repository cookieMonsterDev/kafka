import { compactArray, compactString, defineRequest, field, flexibleObject, int8, int32, int64 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * ListOffsets Request (Version: 6) => replica_id isolation_level [topics] TAG_BUFFER
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partitions => partition current_leader_epoch timestamp TAG_BUFFER
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       timestamp => INT64
 *
 * First flexible version (KIP-482). Same fields as v4/v5; compact types + TAG_BUFFER on every
 * struct. Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 */
const requestSchema = flexibleObject([
  field('replicaId', int32),
  field('isolationLevel', int8),
  field(
    'topics',
    compactArray(
      flexibleObject([
        field('topic', compactString),
        field(
          'partitions',
          compactArray(
            flexibleObject([field('partition', int32), field('currentLeaderEpoch', int32), field('timestamp', int64)]),
          ),
        ),
      ]),
    ),
  ),
]);

export function createFlexibleListOffsetsRequest(apiVersion: 6 | 7 | 8) {
  return defineRequest({
    apiKey: API_KEYS.ListOffsets,
    apiVersion,
    apiName: 'ListOffsets',
    schema: requestSchema,
  });
}

export const listOffsetsRequestV6 = createFlexibleListOffsetsRequest(6);
