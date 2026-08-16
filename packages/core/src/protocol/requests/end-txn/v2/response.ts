/**
 * EndTxn Response (Version: 2) — same wire as v1.
 * May return PRODUCER_FENCED.
 */
export { endTxnResponseV1 as endTxnResponseV2 } from '../v1/response';
export type { EndTxnResponseV1Body as EndTxnResponseV2Body } from '../v1/response';
