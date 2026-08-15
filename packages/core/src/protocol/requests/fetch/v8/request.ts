import type { RequestDefinition } from '../../../schema.js';
import type { FetchRequestOptions } from '../shared.js';
import { fetchRequestV7 } from '../v7/request.js';

/**
 * Quota violation brokers send out responses before throttling.
 * @see https://cwiki.apache.org/confluence/display/KAFKA/KIP-219+-+Improve+quota+communication
 *
 * Fetch Request (Version: 8) - wire shape identical to v7.
 */
export const fetchRequestV8 = (options: FetchRequestOptions): RequestDefinition => ({
  ...fetchRequestV7(options),
  apiVersion: 8,
});
