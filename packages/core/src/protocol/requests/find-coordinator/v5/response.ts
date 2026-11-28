/**
 * FindCoordinator Response (Version: 5) — wire shape and parsing identical to v4. The bump
 * advertises TRANSACTION_ABORTABLE (KIP-890).
 */
export { findCoordinatorResponseV4 as findCoordinatorResponseV5 } from '../v4/response';
export type { FindCoordinatorResponseV4Body as FindCoordinatorResponseV5Body } from '../v4/response';
