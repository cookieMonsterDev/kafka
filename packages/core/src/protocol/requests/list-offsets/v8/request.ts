import { createFlexibleListOffsetsRequest } from '../v6/request';

/**
 * ListOffsets Request (Version: 8) => replica_id isolation_level [topics] TAG_BUFFER
 *
 * Same wire as v6/v7. Adds timestamp -4 (earliest local offset, KIP-405).
 */
export const listOffsetsRequestV8 = createFlexibleListOffsetsRequest(8);
