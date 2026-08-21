import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 10) — wire shape identical to v9 (KIP-951).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceRequestV10 = (options: ProduceRequestOptions): RequestDefinition =>
  createProduceRequest(10, options);
