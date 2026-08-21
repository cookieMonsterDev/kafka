import { KafkaAggregateError, KafkaProtocolError, KafkaUpdateFeaturesError } from '../../../../errors';
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

export interface UpdatableFeatureResult {
  feature: string;
  errorCode: number;
  errorMessage: string | null;
}

export interface UpdateFeaturesResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  errorCode: number;
  errorMessage: string | null;
  results: UpdatableFeatureResult[];
}

const resultSchema = flexibleObject([
  field('feature', compactString),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
export const responseSchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('results', compactArray(resultSchema)),
]);

function protocolError(errorCode: number, errorMessage: string | null): KafkaProtocolError {
  const error = createErrorFromCode(errorCode);
  if (errorMessage == null) return error;
  return new KafkaProtocolError({
    message: errorMessage,
    type: error.type,
    code: error.code,
    retriable: error.retriable,
  });
}

export const updateFeaturesResponseV0: ResponseDefinition<UpdateFeaturesResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    if (failure(data.errorCode)) throw protocolError(data.errorCode, data.errorMessage);

    const failures = data.results.filter(({ errorCode }) => failure(errorCode));
    if (failures.length > 0) {
      throw new KafkaAggregateError(
        'Feature update errors',
        failures.map(
          ({ feature, errorCode, errorMessage }) =>
            new KafkaUpdateFeaturesError(protocolError(errorCode, errorMessage), feature),
        ),
      );
    }
    return data;
  },
};
