import {
  compactArray,
  compactString,
  defineRequest,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import { API_KEYS } from '../../api-keys';

/**
 * AddPartitionsToTxn Request (Version: 3) => transactional_id producer_id producer_epoch [topics] TAG_BUFFER
 *   transactional_id => COMPACT_STRING
 *   producer_id => INT64
 *   producer_epoch => INT16
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => INT32
 *
 * First flexible version (KIP-482). Same fields as v0–v2. Request header v2's trailing
 * TAG_BUFFER is written by `createRequest`, not here.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(int32))]);
export const requestSchema = flexibleObject([
  field('transactionalId', compactString),
  field('producerId', int64),
  field('producerEpoch', int16),
  field('topics', compactArray(topicSchema)),
]);

export const addPartitionsToTxnRequestV3 = defineRequest({
  apiKey: API_KEYS.AddPartitionsToTxn,
  apiVersion: 3,
  apiName: 'AddPartitionsToTxn',
  schema: requestSchema,
});
