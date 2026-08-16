import { createFlexibleCreatePartitionsRequest } from '../v2/request';

/**
 * CreatePartitions Request (Version: 3) — same wire as v2 (flexible).
 * The bump may return THROTTLING_QUOTA_EXCEEDED on the response (KIP-599).
 */
export const createPartitionsRequestV3 = createFlexibleCreatePartitionsRequest(3);
