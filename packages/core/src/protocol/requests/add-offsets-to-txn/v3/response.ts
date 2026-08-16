import { Decoder } from '../../../decoder';
import { field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { addOffsetsToTxnResponseV2, type AddOffsetsToTxnResponseV2Body } from '../v2/response';

export type AddOffsetsToTxnResponseV3Body = AddOffsetsToTxnResponseV2Body;

/**
 * AddOffsetsToTxn Response (Version: 3) => throttle_time_ms error_code TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *
 * First flexible version (KIP-482). Same fields as v0–v2. Quota timing follows v1 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const bodySchema = flexibleObject([field('throttleTime', int32), field('errorCode', int16)]);

export const addOffsetsToTxnResponseV3: ResponseDefinition<AddOffsetsToTxnResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await addOffsetsToTxnResponseV2.parse(data);
    return data;
  },
};
