import type { RequestDefinition } from '../../../schema.js';
import { createProduceRequest, type ProduceRequestOptions } from '../shared.js';

/**
 * Produce Request (Version: 5) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * Wire shape identical to v3-v4; only the response gains `log_start_offset`.
 */
export const produceRequestV5 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(5, options);
