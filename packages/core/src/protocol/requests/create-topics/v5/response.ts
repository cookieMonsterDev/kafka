import { KafkaAggregateError, KafkaCreateTopicError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  boolean,
  compactArray,
  compactNullableArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface CreateTopicResultConfig {
  name: string;
  value: string | null;
  readOnly: boolean;
  configSource: number;
  isSensitive: boolean;
}

export interface CreateTopicsResponseV5TopicError {
  topic: string;
  errorCode: number;
  errorMessage: string | null;
  numPartitions: number;
  replicationFactor: number;
  configs: CreateTopicResultConfig[] | null;
}

export interface CreateTopicsResponseV5Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  topicErrors: CreateTopicsResponseV5TopicError[];
}

/**
 * CreateTopics Response (Version: 5) => throttle_time_ms [topics] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   topics => name error_code error_message num_partitions replication_factor [configs] TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *     num_partitions => INT32
 *     replication_factor => INT16
 *     configs => name value read_only config_source is_sensitive TAG_BUFFER
 *       name => COMPACT_STRING
 *       value => COMPACT_NULLABLE_STRING
 *       read_only => BOOLEAN
 *       config_source => INT8
 *       is_sensitive => BOOLEAN
 *
 * First flexible version (KIP-482). Adds topic configs (KIP-525). `topic_config_error_code` is
 * tagged field 0 and skipped via the empty TAG_BUFFER. Decoded topic results keep `topic` (not
 * `name`) to match this client. Quota timing follows v3 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const configSchema = flexibleObject([
  field('name', compactString),
  field('value', compactNullableString),
  field('readOnly', boolean),
  field('configSource', int8),
  field('isSensitive', boolean),
]);
const topicErrorSchema = flexibleObject([
  field('topic', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('numPartitions', int32),
  field('replicationFactor', int16),
  field('configs', compactNullableArray(configSchema)),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topicErrors', compactArray(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const createTopicsResponseV5: ResponseDefinition<CreateTopicsResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return {
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      topicErrors: [...decoded.topicErrors].sort(topicNameComparator),
    };
  },
  parse: async (data) => {
    const topicsWithError = data.topicErrors.filter(({ errorCode }) => failure(errorCode));
    if (topicsWithError.length > 0) {
      throw new KafkaAggregateError(
        'Topic creation errors',
        topicsWithError.map((error) => new KafkaCreateTopicError(createErrorFromCode(error.errorCode), error.topic)),
      );
    }
    return data;
  },
};
