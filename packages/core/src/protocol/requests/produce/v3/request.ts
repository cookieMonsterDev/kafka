import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

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
 * The first version RecordBatch v2 (KIP-98) became mandatory. Versions 0-2 send the legacy
 * MessageSet format (see `produce/v0`–`v2`).
 */
export const produceRequestV3 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(3, options);
