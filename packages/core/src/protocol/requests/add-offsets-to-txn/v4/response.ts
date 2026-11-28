/**
 * AddOffsetsToTxn Response (Version: 4) — wire shape and parsing identical to v3. The bump
 * advertises TRANSACTION_ABORTABLE (KIP-890).
 */
export { addOffsetsToTxnResponseV3 as addOffsetsToTxnResponseV4 } from '../v3/response';
export type { AddOffsetsToTxnResponseV3Body as AddOffsetsToTxnResponseV4Body } from '../v3/response';
