import { array, defineRequest, field, int16, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * AddPartitionsToTxn Request (Version: 0) => transactional_id producer_id producer_epoch [topics]
 *   transactional_id => STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => INT32
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
const requestSchema = object([
  field('transactionalId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', array(topicSchema)),
]);

export const addPartitionsToTxnRequestV0 = defineRequest({
  apiKey: API_KEYS.AddPartitionsToTxn,
  apiVersion: 0,
  apiName: 'AddPartitionsToTxn',
  schema: requestSchema,
});
