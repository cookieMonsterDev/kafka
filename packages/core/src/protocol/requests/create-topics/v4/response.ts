/**
 * CreateTopics Response (Version: 4) — same wire as v3 (KIP-219 client-side throttle).
 * KIP-464 is a request/broker-semantics change; the response fields do not change.
 */
export { createTopicsResponseV3 as createTopicsResponseV4 } from '../v3/response';
export type { CreateTopicsResponseV3Body as CreateTopicsResponseV4Body } from '../v3/response';
