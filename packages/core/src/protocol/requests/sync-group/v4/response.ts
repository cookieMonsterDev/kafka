import { Decoder } from '../../../decoder';
import { compactBytes, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { syncGroupResponseV2, type SyncGroupResponseV2Body } from '../v2/response';

export type SyncGroupResponseV4Body = SyncGroupResponseV2Body;

const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('memberAssignment', compactBytes),
]);

/**
 * SyncGroup Response (Version: 4) => throttle_time_ms error_code member_assignment TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   member_assignment => COMPACT_BYTES
 *
 * First flexible version (KIP-482). Same fields as v3. Quota timing follows v2 (KIP-219).
 * Response header v1's trailing TAG_BUFFER is skipped by `Connection` before `decode()` runs.
 */
export const syncGroupResponseV4: ResponseDefinition<SyncGroupResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await syncGroupResponseV2.parse(data);
    return data;
  },
};
