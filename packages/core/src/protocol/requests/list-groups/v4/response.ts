import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { listGroupsResponseV3 } from '../v3/response';

export interface ListGroupsResponseV4Body {
  errorCode: number;
  groups: { groupId: string; protocolType: string; groupState: string }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * ListGroups Response (Version: 4) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   groups => group_id protocol_type group_state TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 *     group_state => COMPACT_STRING
 *
 * Adds `groupState` on each group (KIP-518). Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('protocolType', compactString),
  field('groupState', compactString),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('groups', compactArray(groupSchema)),
]);

export const listGroupsResponseV4: ResponseDefinition<ListGroupsResponseV4Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await listGroupsResponseV3.parse(data);
    return data;
  },
};
