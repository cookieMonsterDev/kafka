/**
 * CreatePartitions Response (Version: 3) — same wire as v2 (flexible).
 * May return THROTTLING_QUOTA_EXCEEDED (KIP-599).
 */
export { createPartitionsResponseV2 as createPartitionsResponseV3 } from '../v2/response';
export type { CreatePartitionsResponseV2Body as CreatePartitionsResponseV3Body } from '../v2/response';
