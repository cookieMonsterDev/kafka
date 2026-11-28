import { createFlexibleListOffsetsRequest } from '../v6/request';

/**
 * ListOffsets Request (Version: 9) => replica_id isolation_level [topics] TAG_BUFFER
 *
 * Same wire as v6–v8. Adds timestamp -5 (latest tiered offset, KIP-1005).
 */
export const listOffsetsRequestV9 = createFlexibleListOffsetsRequest(9);
