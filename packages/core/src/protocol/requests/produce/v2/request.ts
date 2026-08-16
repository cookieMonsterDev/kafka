import type { RequestDefinition } from '../../../schema';
import { createMessageSetProduceRequest } from '../message-set';
import type { ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 2) => acks timeout [topic_data]
 *
 * MessageSet magic 1 (per-message timestamp). Compression, when set, wraps the inner set
 * in a single compressed message. The bump also indicates the client can parse the
 * timestamp field in the Produce response.
 */
export const produceRequestV2 = (options: ProduceRequestOptions): RequestDefinition =>
  createMessageSetProduceRequest(2, options);
