import { createFlexibleCreateTopicsRequest } from '../v5/request';

/**
 * CreateTopics Request (Version: 7) — same wire as v5/v6 (flexible).
 * The bump adds `topicId` on the response.
 */
export const createTopicsRequestV7 = createFlexibleCreateTopicsRequest(7);
