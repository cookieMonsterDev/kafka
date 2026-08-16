import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 9) => transactional_id acks timeout [topic_data] TAG_BUFFER
 *   transactional_id => COMPACT_NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     data => partition record_set TAG_BUFFER
 *       partition => INT32
 *       record_set => COMPACT_RECORDS
 *
 * First flexible Produce version (KIP-482). Request header v2 is added by `createRequest`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceRequestV9 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(9, options);
