import type { RequestDefinition } from '../../../schema.js';
import { createProduceRequest, type ProduceRequestOptions } from '../shared.js';

/**
 * Produce Request (Version: 6) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * The version bump signals that on quota violation the broker sends the response before
 * throttling (KIP-219). Wire shape is otherwise identical to v5.
 */
export const produceRequestV6 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(6, options);
