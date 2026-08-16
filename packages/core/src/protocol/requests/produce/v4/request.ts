import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 4) => transactional_id acks timeout [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * Wire shape identical to v3; the bump only enables KIP-219 quota-timing semantics on the
 * response side (see v6's remap of throttle_time_ms).
 */
export const produceRequestV4 = (options: ProduceRequestOptions): RequestDefinition => createProduceRequest(4, options);
