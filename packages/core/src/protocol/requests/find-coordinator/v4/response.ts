import { Decoder } from '../../../decoder';
import { createErrorFromCode, failIfVersionNotSupported, failure } from '../../../error-codes';
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

export interface FindCoordinatorResponseV4Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  coordinators: {
    key: string;
    nodeId: number;
    host: string;
    port: number;
    errorCode: number;
    errorMessage: string | null;
  }[];
}

/**
 * FindCoordinator Response (Version: 4) => throttle_time_ms [coordinators] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   coordinators => key node_id host port error_code error_message TAG_BUFFER
 *     key => COMPACT_STRING
 *     node_id => INT32
 *     host => COMPACT_STRING
 *     port => INT32
 *     error_code => INT16
 *     error_message => COMPACT_NULLABLE_STRING
 *
 * One result per requested key (KIP-699), each with its own error code. Quota timing follows v2
 * (KIP-219). Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()`
 * runs.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const coordinatorSchema = flexibleObject([
  field('key', compactString),
  field('nodeId', int32),
  field('host', compactString),
  field('port', int32),
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('coordinators', compactArray(coordinatorSchema)),
]);

export const findCoordinatorResponseV4: ResponseDefinition<FindCoordinatorResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  // Like v0–v3, a failed key fails the whole lookup, so the caller retries or reports it once.
  parse: async (data) => {
    for (const { errorCode } of data.coordinators) {
      failIfVersionNotSupported(errorCode);
      if (failure(errorCode)) throw createErrorFromCode(errorCode);
    }
    return data;
  },
};
