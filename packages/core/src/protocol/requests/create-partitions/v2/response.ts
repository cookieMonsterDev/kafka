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
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import type { CreatePartitionsResponseV1Body } from '../v1/response';

export type CreatePartitionsResponseV2Body = CreatePartitionsResponseV1Body;

/**
 * CreatePartitions Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   results => name error_code error_message TAG_BUFFER
 *     name => COMPACT_STRING
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * First flexible version (KIP-482). Same fields as v0/v1. Decoded results stay `topicErrors` /
 * `topic` to match this client. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicErrorSchema = flexibleObject([
  field('topic', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([field('throttleTime', int32), field('topicErrors', compactArray(topicErrorSchema))]);

const topicNameComparator = (a: { topic: string }, b: { topic: string }): number => a.topic.localeCompare(b.topic);

export const createPartitionsResponseV2: ResponseDefinition<CreatePartitionsResponseV2Body> = {
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
      const [first] = topicsWithError;
      if (first) throw createErrorFromCode(first.errorCode);
    }
    return data;
  },
};
