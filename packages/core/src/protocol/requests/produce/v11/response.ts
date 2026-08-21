/**
 * Produce Response (Version: 11) — wire shape and parsing identical to v10. The bump
 * advertises TRANSACTION_ABORTABLE (KIP-890).
 */
export { produceResponseV10 as produceResponseV11 } from '../v10/response';
export type { ProduceResponseV10Body as ProduceResponseV11Body } from '../v10/response';
