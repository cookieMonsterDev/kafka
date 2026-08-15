import type { RequestDefinition } from '../../../schema.js';
import type { FetchRequestOptions } from '../shared.js';
import { fetchRequestV9 } from '../v9/request.js';

/**
 * ZSTD Compression.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-110%3A+Add+Codec+for+ZStandard+Compression
 *
 * Fetch Request (Version: 10) - wire shape identical to v9.
 */
export const fetchRequestV10 = (options: FetchRequestOptions): RequestDefinition => ({
  ...fetchRequestV9(options),
  apiVersion: 10,
});
