import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import type { ResponseDefinition } from '../../../schema';

export interface ListPartitionReassignmentsResponseV0Partition {
  partition: number;
  replicas: number[];
  addingReplicas: number[];
  removingReplicas: number[];
}

export interface ListPartitionReassignmentsResponseV0Topic {
  name: string;
  partitions: ListPartitionReassignmentsResponseV0Partition[];
}

export interface ListPartitionReassignmentsResponseV0Body {
  throttleTime: number;
  errorCode: number;
  topics: ListPartitionReassignmentsResponseV0Topic[];
}

function decodeReplicas(decoder: Decoder): number {
  return decoder.readInt32();
}

function decodePartition(decoder: Decoder): ListPartitionReassignmentsResponseV0Partition {
  const partition = {
    partition: decoder.readInt32(),
    replicas: decoder.readUVarIntArray(decodeReplicas) ?? [],
    addingReplicas: decoder.readUVarIntArray(decodeReplicas) ?? [],
    removingReplicas: decoder.readUVarIntArray(decodeReplicas) ?? [],
  };
  decoder.readTaggedFields();
  return partition;
}

function decodeTopic(decoder: Decoder): ListPartitionReassignmentsResponseV0Topic {
  const topic = {
    name: decoder.readUVarIntString() ?? '',
    partitions: decoder.readUVarIntArray(decodePartition) ?? [],
  };
  decoder.readTaggedFields();
  return topic;
}

/**
 * ListPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index [replicas] [adding_replicas] [removing_replicas] TAG_BUFFER
 *       partition_index => INT32
 *       replicas => INT32
 *       adding_replicas => INT32
 *       removing_replicas => INT32
 *
 * Flexible-version, hand-written for the same reason as the request.
 */
export const listPartitionReassignmentsResponseV0: ResponseDefinition<ListPartitionReassignmentsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    decoder.readTaggedFields();
    const throttleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    decoder.readUVarIntString(); // error_message, unused
    return { throttleTime, errorCode, topics: decoder.readUVarIntArray(decodeTopic) ?? [] };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    return data;
  },
};
