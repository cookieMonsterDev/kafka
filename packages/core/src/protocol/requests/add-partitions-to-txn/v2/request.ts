import { array, defineRequest, field, int16, int32, int64, object, string } from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * AddPartitionsToTxn Request (Version: 2) — same wire as v0/v1.
 * The bump may return PRODUCER_FENCED on the response.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = object([field('topic', string), field('partitions', array(int32))]);
const requestSchema = object([
  field('transactionalId', string),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', array(topicSchema)),
]);

export const addPartitionsToTxnRequestV2 = defineRequest({
  apiKey: API_KEYS.AddPartitionsToTxn,
  apiVersion: 2,
  apiName: 'AddPartitionsToTxn',
  schema: requestSchema,
});
