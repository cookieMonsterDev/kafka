import { Decoder } from '../../../decoder';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  object,
  taggedFields,
  type FieldCodec,
  type ResponseDefinition,
} from '../../../schema';
import {
  parseProduceResponse,
  readProducePartitionTaggedFields,
  readProduceResponseNodeEndpoints,
  type LeaderIdAndEpoch,
  type ProduceNodeEndpoint,
} from '../shared';
import type { ProduceRecordError } from '../v8/response';

export interface ProducePartitionResponseV9 {
  partition: number;
  errorCode: number;
  baseOffset: bigint;
  logAppendTime: bigint;
  logStartOffset: bigint;
  recordErrors: ProduceRecordError[];
  errorMessage: string | null;
  /** KIP-951, tag 0 (v10+); `null` on a v9 broker or when the leader hasn't changed. */
  currentLeader: LeaderIdAndEpoch | null;
}

export interface ProduceResponseV9Body {
  topics: {
    topicName: string;
    partitions: ProducePartitionResponseV9[];
  }[];
  throttleTime: number;
  clientSideThrottleTime: number;
  /** KIP-951, tag 0 (v10+); `[]` on a v9 broker or when no unknown leader was reported. */
  nodeEndpoints: ProduceNodeEndpoint[];
}

const partitionBodySchema = object([
  field('partition', int32),
  field('errorCode', int16),
  field('baseOffset', int64),
  field('logAppendTime', int64),
  field('logStartOffset', int64),
  field(
    'recordErrors',
    compactArray(flexibleObject([field('batchIndex', int32), field('batchIndexErrorMessage', compactNullableString)])),
  ),
  field('errorMessage', compactNullableString),
]);

/**
 * Like `flexibleObject()`, but the trailing TAG_BUFFER carries KIP-951's CurrentLeader (tag 0)
 * instead of being skipped.
 */
export const producePartitionSchemaV9: FieldCodec<ProducePartitionResponseV9> = {
  write: (e, value) => {
    partitionBodySchema.write(e, value);
    taggedFields.write(e, null);
  },
  read: (d) => ({ ...partitionBodySchema.read(d), currentLeader: readProducePartitionTaggedFields(d) }),
};

const bodySchema = object([
  field(
    'topics',
    compactArray(
      flexibleObject([field('topicName', compactString), field('partitions', compactArray(producePartitionSchemaV9))]),
    ),
  ),
  field('throttleTime', int32),
]);

/**
 * Produce Response (Version: 9) => [responses] throttle_time_ms TAG_BUFFER
 *   responses => topic [partition_responses] TAG_BUFFER
 *     topic => COMPACT_STRING
 *     partition_responses => partition error_code base_offset log_append_time log_start_offset
 *                            [record_errors] error_message TAG_BUFFER
 *       partition => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time => INT64
 *       log_start_offset => INT64
 *       record_errors => batch_index batch_index_error_message TAG_BUFFER
 *         batch_index => INT32
 *         batch_index_error_message => COMPACT_NULLABLE_STRING
 *       error_message => COMPACT_NULLABLE_STRING
 *   throttle_time_ms => INT32
 *
 * Flexible version of v8 (KIP-482). throttle_time_ms stays INT32. CurrentLeader (partition tag
 * 0) and NodeEndpoints (response tag 0) are decoded from v10+ (KIP-951); a v9 broker never sends
 * either tag, so both default to their "no leader change" values.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
export const produceResponseV9: ResponseDefinition<ProduceResponseV9Body> = {
  decode: async (rawData) => {
    const decoder = new Decoder(rawData);
    const decoded = bodySchema.read(decoder);
    const nodeEndpoints = readProduceResponseNodeEndpoints(decoder);
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime, nodeEndpoints };
  },
  parse: parseProduceResponse,
};
