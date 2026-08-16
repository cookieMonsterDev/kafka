/**
 * DeleteTopics Response (Version: 2) — same fields as v1.
 * Starting in v2, brokers send the response before throttling (KIP-219); v1 already remaps
 * wire `throttleTime` onto `clientSideThrottleTime`.
 */
export { deleteTopicsResponseV1 as deleteTopicsResponseV2 } from '../v1/response';
export type { DeleteTopicsResponseV1Body as DeleteTopicsResponseV2Body } from '../v1/response';
