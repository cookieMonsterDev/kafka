import type { RequestDefinition } from '../../../schema.js';
import type { FetchRequestOptions } from '../shared.js';
import { fetchRequestV5 } from '../v5/request.js';

/**
 * Fetch Request (Version: 6) => replica_id max_wait_time min_bytes max_bytes isolation_level [topics]
 *   (wire shape identical to v5)
 */
export const fetchRequestV6 = (options: FetchRequestOptions): RequestDefinition => ({
  ...fetchRequestV5(options),
  apiVersion: 6,
});
