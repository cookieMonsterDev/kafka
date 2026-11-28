/**
 * EndTxn Response (Version: 4) — wire shape and parsing identical to v3. The bump advertises
 * TRANSACTION_ABORTABLE (KIP-890).
 */
export { endTxnResponseV3 as endTxnResponseV4 } from '../v3/response';
export type { EndTxnResponseV3Body as EndTxnResponseV4Body } from '../v3/response';
