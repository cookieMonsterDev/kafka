import type { RequestDefinition } from '../../../schema';
import { createMessageSetProduceRequest } from '../message-set';
import type { ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 0) => acks timeout [topic_data]
 *   acks => INT16
 *   timeout => INT32
 *   topic_data => topic [data]
 *     topic => STRING
 *     data => partition record_set
 *       partition => INT32
 *       record_set => RECORDS
 *
 * MessageSet magic 0. Compression is not applied at this version.
 */
export const produceRequestV0 = (options: ProduceRequestOptions): RequestDefinition =>
  createMessageSetProduceRequest(0, options);
