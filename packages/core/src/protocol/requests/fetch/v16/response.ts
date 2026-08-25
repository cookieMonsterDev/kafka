import { fetchResponseV13 } from '../v13/response';

/**
 * Fetch Response (Version: 16) — adds tagged NodeEndpoints (KIP-951), decoded by `fetchResponseV13`.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV16 = fetchResponseV13;
