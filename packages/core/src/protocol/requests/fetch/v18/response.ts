import { fetchResponseV13 } from '../v13/response';

/**
 * Fetch Response (Version: 18) — no response field changes (KIP-1166).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV18 = fetchResponseV13;
