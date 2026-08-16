import { createFlexibleListOffsetsRequest } from '../v6/request';

/**
 * ListOffsets Request (Version: 7) => replica_id isolation_level [topics] TAG_BUFFER
 *
 * Same wire as v6. Adds timestamp type -3 (max timestamp, KIP-734). No extra fields.
 */
export const listOffsetsRequestV7 = createFlexibleListOffsetsRequest(7);
