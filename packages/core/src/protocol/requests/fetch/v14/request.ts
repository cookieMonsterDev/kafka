import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 14) — wire shape identical to v13. Adds
 * OffsetMovedToTieredStorageException (KIP-405).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV14 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(14, options);
