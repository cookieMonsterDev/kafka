import { compactArray, compactString, defineRequest, field, flexibleObject, int32 } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export {
  type OffsetDeletePartition,
  type OffsetDeleteRequestV0Fields as OffsetDeleteRequestV1Fields,
  type OffsetDeleteTopic,
} from '../v0/request';

/**
 * OffsetDelete Request (Version: 1) => group_id [topics] TAG_BUFFER
 *   group_id => COMPACT_STRING
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index TAG_BUFFER
 *       partition_index => INT32
 *
 * Flexible-version API. Request header v2's trailing TAG_BUFFER is written by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const partitionSchema = flexibleObject([field('partitionIndex', int32)]);
const topicSchema = flexibleObject([field('name', compactString), field('partitions', compactArray(partitionSchema))]);
export const requestSchema = flexibleObject([
  field('groupId', compactString),
  field('topics', compactArray(topicSchema)),
]);

export const offsetDeleteRequestV1 = defineRequest({
  apiKey: API_KEYS.OffsetDelete,
  apiVersion: 1,
  apiName: 'OffsetDelete',
  schema: requestSchema,
});
