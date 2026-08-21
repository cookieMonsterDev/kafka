import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 17) — wire shape identical to v15 for consumers. ReplicaDirectoryId
 * is tagged field 0 on each partition (KIP-853) and is omitted at the default.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV17 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(17, options);
