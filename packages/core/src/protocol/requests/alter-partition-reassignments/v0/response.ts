import { KafkaJSAggregateError, KafkaJSAlterPartitionReassignmentsError } from '../../../../errors.js';
import { Decoder } from '../../../decoder.js';
import { createErrorFromCode, failure } from '../../../error-codes.js';
import type { ResponseDefinition } from '../../../schema.js';

export interface AlterPartitionReassignmentsResponseV0Partition {
  partition: number;
  errorCode: number;
}

export interface AlterPartitionReassignmentsResponseV0Topic {
  topic: string;
  partitions: AlterPartitionReassignmentsResponseV0Partition[];
}

export interface AlterPartitionReassignmentsResponseV0Body {
  throttleTime: number;
  errorCode: number;
  responses: AlterPartitionReassignmentsResponseV0Topic[];
}

function decodePartition(decoder: Decoder): AlterPartitionReassignmentsResponseV0Partition {
  const partition = { partition: decoder.readInt32(), errorCode: decoder.readInt16() };
  decoder.readUVarIntString(); // error_message, unused
  decoder.readTaggedFields();
  return partition;
}

function decodeResponse(decoder: Decoder): AlterPartitionReassignmentsResponseV0Topic {
  const response = {
    topic: decoder.readUVarIntString() ?? '',
    partitions: decoder.readUVarIntArray(decodePartition) ?? [],
  };
  decoder.readTaggedFields();
  return response;
}

/**
 * AlterPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [responses] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => COMPACT_NULLABLE_STRING
 *   responses => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code error_message TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *       error_message => COMPACT_NULLABLE_STRING
 *
 * Flexible-version, hand-written for the same reason as the request (see `v0/request.ts`).
 */
export const alterPartitionReassignmentsResponseV0: ResponseDefinition<AlterPartitionReassignmentsResponseV0Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    decoder.readTaggedFields();
    const throttleTime = decoder.readInt32();
    const errorCode = decoder.readInt16();
    decoder.readUVarIntString(); // error_message, unused
    return { throttleTime, errorCode, responses: decoder.readUVarIntArray(decodeResponse) ?? [] };
  },
  parse: async (data) => {
    // A request-level failure has no specific topic/partition to attach, unlike the per-partition
    // case below, so it surfaces as a plain protocol error rather than KafkaJSAlterPartitionReassignmentsError
    // (kafkajs's own code calls that error's constructor with only the error argument here, relying
    // on JS to leave `topic`/`partition` as `undefined`; our stronger typing makes both required).
    if (failure(data.errorCode)) {
      throw createErrorFromCode(data.errorCode);
    }

    const topicPartitionsWithError = data.responses.flatMap(({ partitions, topic }) =>
      partitions.filter((partition) => failure(partition.errorCode)).map((partition) => ({ ...partition, topic })),
    );

    if (topicPartitionsWithError.length > 0) {
      throw new KafkaJSAggregateError(
        'Errors altering partition reassignments',
        topicPartitionsWithError.map(
          ({ topic, partition, errorCode }) =>
            new KafkaJSAlterPartitionReassignmentsError(createErrorFromCode(errorCode), topic, partition),
        ),
      );
    }

    return data;
  },
};
