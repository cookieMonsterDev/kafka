import { createFlexibleDeleteTopicsRequest } from '../v4/request';

/**
 * DeleteTopics Request (Version: 5) — same wire as v4 (flexible compact topic names).
 * The bump adds ErrorMessage on the response and may return THROTTLING_QUOTA_EXCEEDED (KIP-599).
 */
export const deleteTopicsRequestV5 = createFlexibleDeleteTopicsRequest(5);
