import { array, defineRequest, field, int16, int32, int64, nullableString, object, string } from '../../../schema.js';
import { API_KEYS } from '../../api-keys.js';

/**
 * TxnOffsetCommit Request (Version: 0) => transactional_id group_id producer_id producer_epoch [topics]
 *   transactional_id => STRING
 *   group_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition offset metadata
 *       partition => INT32
 *       offset => INT64
 *       metadata => NULLABLE_STRING
 */
const partitionSchema = object([field('partition', int32), field('offset', int64), field('metadata', nullableString)]);
const topicSchema = object([field('topic', string), field('partitions', array(partitionSchema))]);
const requestSchema = object([
  field('transactionalId', string),
  field('groupId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', array(topicSchema)),
]);

export const txnOffsetCommitRequestV0 = defineRequest({
  apiKey: API_KEYS.TxnOffsetCommit,
  apiVersion: 0,
  apiName: 'TxnOffsetCommit',
  schema: requestSchema,
});
