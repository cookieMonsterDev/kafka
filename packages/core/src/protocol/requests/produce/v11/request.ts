import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 11) — wire shape identical to v10. Adds support for the
 * TRANSACTION_ABORTABLE error code (KIP-890).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceRequestV11 = (options: ProduceRequestOptions): RequestDefinition =>
  createProduceRequest(11, options);
