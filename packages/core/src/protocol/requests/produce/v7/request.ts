import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 7) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * The version bump indicates ZSTD capability (KIP-110). Wire shape is otherwise identical to v6.
 */
export const produceRequestV7 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(7, options);
