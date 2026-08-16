import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 8) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * Wire shape identical to v7. The bump adds record-level errors on the response (KIP-467).
 */
export const produceRequestV8 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(8, options);
