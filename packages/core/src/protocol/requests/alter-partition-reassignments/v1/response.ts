import { Decoder } from '../../../decoder';
import type { ResponseDefinition } from '../../../schema';
import {
  alterPartitionReassignmentsResponseV0,
  type AlterPartitionReassignmentsResponseV0Body,
  decodeTopicResponse,
} from '../v0/response';

export interface AlterPartitionReassignmentsResponseV1Body extends AlterPartitionReassignmentsResponseV0Body {
  allowReplicationFactorChange: boolean;
}

/**
 * AlterPartitionReassignments Response (Version: 1) => throttle_time_ms allow_replication_factor_change error_code error_message [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   allow_replication_factor_change => BOOLEAN
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   responses => see v0
 *
 * v1 echoes whether the request allowed replication factor changes (KIP-860). A partition the
 * request would resize while that is disallowed fails with INVALID_REPLICATION_FACTOR.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const alterPartitionReassignmentsResponseV1: ResponseDefinition<AlterPartitionReassignmentsResponseV1Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const throttleTime = decoder.readInt32();
    const allowReplicationFactorChange = decoder.readBoolean();
    const errorCode = decoder.readInt16();
    decoder.readUVarIntString(); // error_message, unused
    return {
      throttleTime,
      allowReplicationFactorChange,
      errorCode,
      responses: decoder.readUVarIntArray(decodeTopicResponse) ?? [],
    };
  },
  parse: async (data) => {
    await alterPartitionReassignmentsResponseV0.parse(data);
    return data;
  },
};
