import { fetchResponseV13 } from '../v13/response';

/**
 * Fetch Response (Version: 14) — wire shape identical to v13. Adds
 * OffsetMovedToTieredStorageException (KIP-405).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const fetchResponseV14 = fetchResponseV13;
