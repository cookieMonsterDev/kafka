import type { RequestDefinition } from '../../../schema.js';
import { createProduceRequest, type ProduceRequestOptions } from '../shared.js';

/**
 * Produce Request (Version: 3) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * The first version RecordBatch v2 (KIP-98) became mandatory - versions 0-2 sent the legacy
 * message-set format instead and are not implemented here.
 */
export const produceRequestV3 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(3, options);
