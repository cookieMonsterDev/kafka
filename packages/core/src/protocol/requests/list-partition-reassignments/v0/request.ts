import { Encoder } from '../../../encoder';
import type { RequestDefinition } from '../../../schema';
import { API_KEYS } from '../../api-keys';

export interface ListPartitionReassignmentsRequestV0Topic {
  topic: string;
  partitions: number[];
}

export interface ListPartitionReassignmentsRequestV0Options {
  topics?: ListPartitionReassignmentsRequestV0Topic[] | null;
  timeout?: number;
}

function encodeTopic({ topic, partitions }: ListPartitionReassignmentsRequestV0Topic): Encoder {
  return new Encoder()
    .writeUVarIntString(topic)
    .writeUVarIntArray(partitions.map((partition) => new Encoder().writeInt32(partition)))
    .writeUVarIntBytes(undefined);
}

/**
 * ListPartitionReassignments Request (Version: 0) => timeout_ms [topics] TAG_BUFFER
 *   timeout_ms => INT32
 *   topics => name [partition_indexes] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partition_indexes => INT32
 *
 * Flexible-version (see `alter-partition-reassignments/v0/request.ts` for why the leading
 * `writeUVarIntBytes()` stands in for the request header's own TAG_BUFFER).
 *
 * `topics: null` means "list every in-flight reassignment" and is encoded as a compact-array
 * null marker (`0`), not an empty array.
 *
 * @see https://kafka.apache.org/43/operations/basic-kafka-operations/
 */
export const listPartitionReassignmentsRequestV0: (
  values: ListPartitionReassignmentsRequestV0Options,
) => RequestDefinition = ({ topics = null, timeout = 5000 }) => ({
  apiKey: API_KEYS.ListPartitionReassignments,
  apiVersion: 0,
  apiName: 'ListPartitionReassignments',
  encode: () =>
    Promise.resolve(
      new Encoder()
        .writeUVarIntBytes(undefined)
        .writeInt32(timeout)
        .writeUVarIntArray(topics === null ? null : topics.map(encodeTopic))
        .writeUVarIntBytes(undefined),
    ),
});
