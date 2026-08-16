import { createFlexibleCreateTopicsRequest } from '../v5/request';

/**
 * CreateTopics Request (Version: 6) — same wire as v5 (flexible).
 * The bump may return THROTTLING_QUOTA_EXCEEDED on the response (KIP-599).
 */
export const createTopicsRequestV6 = createFlexibleCreateTopicsRequest(6);
