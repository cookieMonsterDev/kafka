/**
 * CreateTopics Response (Version: 6) — same wire as v5 (flexible + KIP-525 configs).
 * May return THROTTLING_QUOTA_EXCEEDED (KIP-599).
 */
export { createTopicsResponseV5 as createTopicsResponseV6 } from '../v5/response';
export type { CreateTopicsResponseV5Body as CreateTopicsResponseV6Body } from '../v5/response';
