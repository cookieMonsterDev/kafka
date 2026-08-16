import type { RequestDefinition } from '../../../schema';
import type { FetchRequestOptions } from '../shared';
import { createFetchRequestV0Style } from '../v0/request';

/**
 * Fetch Request (Version: 2) => replica_id max_wait_time min_bytes [topics]
 *
 * Wire shape identical to v0.
 */
export const fetchRequestV2 = (options: FetchRequestOptions): RequestDefinition =>
  createFetchRequestV0Style(2)(options);
