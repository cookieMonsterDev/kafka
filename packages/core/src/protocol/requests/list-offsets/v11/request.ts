import { createListOffsetsRequestWithTimeout } from '../v10/request';

/**
 * ListOffsets Request (Version: 11) => replica_id isolation_level [topics] timeout_ms TAG_BUFFER
 *
 * Same wire as v10. Adds timestamp -6 (earliest pending upload offset, KIP-1023).
 */
export const listOffsetsRequestV11 = createListOffsetsRequestWithTimeout(11);
