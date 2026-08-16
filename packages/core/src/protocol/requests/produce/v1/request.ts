import type { RequestDefinition } from '../../../schema';
import { createMessageSetProduceRequest } from '../message-set';
import type { ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 1) => acks timeout [topic_data]
 *
 * Wire shape identical to v0. The bump indicates the client can parse quota throttle time
 * in the Produce response.
 */
export const produceRequestV1 = (options: ProduceRequestOptions): RequestDefinition =>
  createMessageSetProduceRequest(1, options);
