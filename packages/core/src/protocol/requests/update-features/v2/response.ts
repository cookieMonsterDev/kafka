import { KafkaProtocolError } from '../../../../errors';
import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import { compactNullableString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';

export interface UpdateFeaturesResponseV2Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  results: [];
}

export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);

export const updateFeaturesResponseV2: ResponseDefinition<UpdateFeaturesResponseV2Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime, results: [] };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) {
      const error = createErrorFromCode(data.errorCode);
      if (data.errorMessage == null) throw error;
      throw new KafkaProtocolError({
        message: data.errorMessage,
        type: error.type,
        code: error.code,
        retriable: error.retriable,
      });
    }
    return data;
  },
};
