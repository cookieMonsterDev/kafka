import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 15) => max_wait_ms min_bytes max_bytes isolation_level
 *                                session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
 *
 * Drops the ReplicaId INT32. ReplicaState (replicaId + replicaEpoch) is tagged field 1 and
 * omitted for consumers (KIP-903 defaults: replicaId -1, replicaEpoch -1).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV15 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(15, options);
