import type { RequestDefinition } from '../../../schema';
import type { FetchRequestOptions } from '../shared';
import { fetchRequestV5 } from '../v5/request';

/**
 * Fetch Request (Version: 6) => replica_id max_wait_time min_bytes max_bytes isolation_level [topics]
 *   (wire shape identical to v5)
 */
export const fetchRequestV6 = (options: FetchRequestOptions): RequestDefinition => ({
  ...fetchRequestV5(options),
  apiVersion: 6,
});
