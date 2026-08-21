import type { RequestDefinition } from '../../../schema';
import { createFetchRequest, type FetchRequestOptions } from '../shared';

/**
 * Fetch Request (Version: 13) => replica_id max_wait_ms min_bytes max_bytes isolation_level
 *                                session_id session_epoch [topics] [forgotten_topics_data] rack_id TAG_BUFFER
 *   topics => topic_id [partitions] TAG_BUFFER
 *     topic_id => UUID
 *
 * Replaces topic names with topic IDs (KIP-516). May return UNKNOWN_TOPIC_ID.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchRequestV13 = (options: FetchRequestOptions): RequestDefinition => createFetchRequest(13, options);
