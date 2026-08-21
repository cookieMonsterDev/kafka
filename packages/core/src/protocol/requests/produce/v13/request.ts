import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 13) => transactional_id acks timeout [topic_data] TAG_BUFFER
 *   transactional_id => COMPACT_NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic_id [data] TAG_BUFFER
 *     topic_id => UUID
 *     data => partition record_set TAG_BUFFER
 *       partition => INT32
 *       record_set => COMPACT_RECORDS
 *
 * Replaces topic names with topic IDs (KIP-516). May return UNKNOWN_TOPIC_ID.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceRequestV13 = (options: ProduceRequestOptions): RequestDefinition =>
  createProduceRequest(13, options);
