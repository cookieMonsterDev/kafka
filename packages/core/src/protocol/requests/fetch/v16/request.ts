import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 16) — wire shape identical to v15 (KIP-951).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV16 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(16, options);
