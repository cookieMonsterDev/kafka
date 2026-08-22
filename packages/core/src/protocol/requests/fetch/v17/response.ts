import { fetchResponseV13 } from '../v13/response';

/**
 * Fetch Response (Version: 17) — no response field changes (KIP-853).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV17 = fetchResponseV13;
