import { Encoder } from '../../../encoder';
import type { RequestDefinition } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface AlterPartitionReassignmentsRequestV0Partition {
  partition: number;
  replicas: number[];
}

export interface AlterPartitionReassignmentsRequestV0Topic {
  topic: string;
  partitionAssignment: AlterPartitionReassignmentsRequestV0Partition[];
}

export interface AlterPartitionReassignmentsRequestV0Options {
  topics: AlterPartitionReassignmentsRequestV0Topic[];
  timeout?: number;
}

function encodePartition({ partition, replicas }: AlterPartitionReassignmentsRequestV0Partition): Encoder {
  return new Encoder()
    .writeInt32(partition)
    .writeUVarIntArray(replicas.map((replica) => new Encoder().writeInt32(replica)))
    .writeUVarIntBytes(undefined);
}

function encodeTopic({ topic, partitionAssignment }: AlterPartitionReassignmentsRequestV0Topic): Encoder {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitionAssignment.map(encodePartition))
    .writeUVarIntBytes(undefined);
}

/**
 * AlterPartitionReassignments Request (Version: 0) => timeout_ms [topics] TAG_BUFFER
 *   timeout_ms => INT32
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index [replicas] TAG_BUFFER
 *       partition_index => INT32
 *       replicas => INT32
 *
 * Flexible-version API (compact strings/arrays, TAG_BUFFER). Hand-written against
 * `Encoder`'s `writeUVarInt*` primitives because this family's body is not yet on the schema DSL.
 * Request header v2's trailing TAG_BUFFER is written by `createRequest`, not here.
 */
export const alterPartitionReassignmentsRequestV0: (
  values: AlterPartitionReassignmentsRequestV0Options,
) => RequestDefinition = ({ topics, timeout = 5000 }) => ({
  apiKey: API_KEYS.AlterPartitionReassignments,
  apiVersion: 0,
  apiName: 'AlterPartitionReassignments',
  encode: () =>
    Promise.resolve(
      new Encoder().writeInt32(timeout).writeUVarIntArray(topics.map(encodeTopic)).writeUVarIntBytes(undefined),
    ),
});
