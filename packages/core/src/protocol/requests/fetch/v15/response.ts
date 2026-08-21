import { fetchResponseV13 } from '../v13/response';

/**
 * Fetch Response (Version: 15) — wire shape identical to v14 (KIP-903).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV15 = fetchResponseV13;
