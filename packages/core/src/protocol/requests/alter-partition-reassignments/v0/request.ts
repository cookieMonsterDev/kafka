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
 * `Encoder`'s `writeUVarInt*` primitives because the schema DSL covers non-flexible shapes only.
 *
 * The leading `writeUVarIntBytes()` before `timeout_ms` isn't in the BNF above — it stands in for
 * the flexible request *header*'s trailing TAG_BUFFER (header version 2, KIP-482), which the
 * generic request-wrapping code writes a plain (non-flexible) header regardless of API and never
 * appends. Since this family's `encode()` output is concatenated directly after the header's
 * `client_id`, prepending one empty tag buffer here reproduces a byte-correct flexible header.
 */
export const alterPartitionReassignmentsRequestV0: (
  values: AlterPartitionReassignmentsRequestV0Options,
) => RequestDefinition = ({ topics, timeout = 5000 }) => ({
  apiKey: API_KEYS.AlterPartitionReassignments,
  apiVersion: 0,
  apiName: 'AlterPartitionReassignments',
  encode: () =>
    Promise.resolve(
      new Encoder()
        .writeUVarIntBytes(undefined)
        .writeInt32(timeout)
        .writeUVarIntArray(topics.map(encodeTopic))
        .writeUVarIntBytes(undefined),
    ),
});
