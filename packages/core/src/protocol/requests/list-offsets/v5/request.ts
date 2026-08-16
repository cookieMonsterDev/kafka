import { defineRequest } from '../../../schema';
import { API_KEYS } from '../../api-keys';
import { listOffsetsRequestSchemaV4 } from '../v4/request';

/**
 * ListOffsets Request (Version: 5) => replica_id isolation_level [topics]
 *   replica_id => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition current_leader_epoch timestamp
 *       partition => INT32
 *       current_leader_epoch => INT32
 *       timestamp => INT64
 *
 * Wire format is identical to v4. The bump adds `leader_epoch` on the response (see v5/response).
 */
export const listOffsetsRequestV5 = defineRequest({
  apiKey: API_KEYS.ListOffsets,
  apiVersion: 5,
  apiName: 'ListOffsets',
  schema: listOffsetsRequestSchemaV4,
});
