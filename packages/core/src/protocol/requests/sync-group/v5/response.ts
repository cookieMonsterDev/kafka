import { Decoder } from '../../../decoder';
import { compactBytes, compactNullableString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { syncGroupResponseV2, type SyncGroupResponseV2Body } from '../v2/response';

export type SyncGroupResponseV5Body = SyncGroupResponseV2Body & {
  protocolType: string | null;
  protocolName: string | null;
};

const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('protocolType', compactNullableString),
  field('protocolName', compactNullableString),
  field('memberAssignment', compactBytes),
]);

/**
 * SyncGroup Response (Version: 5) => throttle_time_ms error_code protocol_type protocol_name
 *                                    member_assignment TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   protocol_type => COMPACT_NULLABLE_STRING
 *   protocol_name => COMPACT_NULLABLE_STRING
 *   member_assignment => COMPACT_BYTES
 *
 * Adds ProtocolType and ProtocolName (KIP-559). Existing `memberAssignment` and error parse stay.
 */
export const syncGroupResponseV5: ResponseDefinition<SyncGroupResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await syncGroupResponseV2.parse(data);
    return data;
  },
};
