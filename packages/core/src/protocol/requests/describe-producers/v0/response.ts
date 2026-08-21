import { KafkaAggregateError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DescribeProducersResponseV0ActiveProducer {
  producerId: bigint;
  producerEpoch: number;
  lastSequence: number;
  lastTimestamp: bigint;
  coordinatorEpoch: number;
  currentTransactionStartOffset: bigint;
}

export interface DescribeProducersResponseV0Partition {
  partition: number;
  errorCode: number;
  errorMessage: string | null;
  activeProducers: DescribeProducersResponseV0ActiveProducer[];
}

export interface DescribeProducersResponseV0Topic {
  topic: string;
  partitions: DescribeProducersResponseV0Partition[];
}

export interface DescribeProducersResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topics: DescribeProducersResponseV0Topic[];
}

/**
 * DescribeProducers Response (Version: 0) => throttle_time_ms [topics] TAG_BUFFER
 *   topics => name [partitions] TAG_BUFFER
 *     name => COMPACT_STRING
 *     partitions => partition_index error_code error_message [active_producers] TAG_BUFFER
 *       partition_index => INT32
 *       error_code => INT16
 *       error_message => COMPACT_NULLABLE_STRING
 *       active_producers => producer_id producer_epoch last_sequence last_timestamp
 *                           coordinator_epoch current_txn_start_offset TAG_BUFFER
 *
 * Flexible from v0. Response header v1's trailing TAG_BUFFER is skipped by `Connection`.
 * Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const activeProducerSchema = flexibleObject([
  field('producerId', int64),
  field('producerEpoch', int32),
  field('lastSequence', int32),
  field('lastTimestamp', int64),
  field('coordinatorEpoch', int32),
  field('currentTransactionStartOffset', int64),
]);

const partitionSchema = flexibleObject([
  field('partition', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('activeProducers', compactArray(activeProducerSchema)),
]);

const topicSchema = flexibleObject([field('topic', compactString), field('partitions', compactArray(partitionSchema))]);

export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('topics', compactArray(topicSchema)),
]);

export const describeProducersResponseV0: ResponseDefinition<DescribeProducersResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    const errors = data.topics.flatMap(({ partitions }) =>
      partitions.filter(({ errorCode }) => failure(errorCode)).map(({ errorCode }) => createErrorFromCode(errorCode)),
    );
    if (errors.length > 0) {
      throw new KafkaAggregateError('Errors describing producers', errors);
    }
    return data;
  },
};
