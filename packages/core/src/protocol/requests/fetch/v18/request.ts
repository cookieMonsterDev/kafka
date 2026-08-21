import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 18) — wire shape identical to v15 for consumers. HighWatermark is
 * tagged field 1 on each partition (KIP-1166) and is omitted when unsupported.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV18 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(18, options);
