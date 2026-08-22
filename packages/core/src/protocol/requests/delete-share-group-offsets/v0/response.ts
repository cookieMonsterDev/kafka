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
  uuid,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface DeleteShareGroupOffsetsTopicResult {
  topicName: string;
  topicId: Buffer;
  errorCode: number;
  errorMessage: string | null;
}

export interface DeleteShareGroupOffsetsResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  responses: DeleteShareGroupOffsetsTopicResult[];
}

/**
 * DeleteShareGroupOffsets Response (Version: 0) => throttle_time_ms error_code error_message
 *                                                    [responses] TAG_BUFFER
 *
 * Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const topicSchema = flexibleObject([
  field('topicName', compactString),
  field('topicId', uuid),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('responses', compactArray(topicSchema)),
]);

export const deleteShareGroupOffsetsResponseV0: ResponseDefinition<DeleteShareGroupOffsetsResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw createErrorFromCode(data.errorCode);
    const failedTopic = data.responses.find(({ errorCode }) => failure(errorCode));
    if (failedTopic) throw createErrorFromCode(failedTopic.errorCode);
    return data;
  },
};
