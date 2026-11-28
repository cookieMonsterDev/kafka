/**
 * TxnOffsetCommit Response (Version: 4) — wire shape and parsing identical to v3. The bump
 * advertises TRANSACTION_ABORTABLE (KIP-890).
 */
export { txnOffsetCommitResponseV3 as txnOffsetCommitResponseV4 } from '../v3/response';
export type { TxnOffsetCommitResponseV3Body as TxnOffsetCommitResponseV4Body } from '../v3/response';
