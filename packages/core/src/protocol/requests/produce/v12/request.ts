import type { RequestDefinition } from '../../../schema';
import { createProduceRequest, type ProduceRequestOptions } from '../shared';

/**
 * Produce Request (Version: 12) — wire shape identical to v11. KIP-890 part 2: when
 * transaction V2 is enabled, Produce also covers AddPartitionsToTxn.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceRequestV12 = (options: ProduceRequestOptions): RequestDefinition =>
  createProduceRequest(12, options);
