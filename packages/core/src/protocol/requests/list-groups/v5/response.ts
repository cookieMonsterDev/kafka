import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { listGroupsResponseV4 } from '../v4/response';

export interface ListGroupsResponseV5Body {
  errorCode: number;
  groups: { groupId: string; protocolType: string; groupState: string; groupType: string }[];
  throttleTime: number;
  clientSideThrottleTime: number;
}

/**
 * ListGroups Response (Version: 5) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   groups => group_id protocol_type group_state group_type TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 *     group_state => COMPACT_STRING
 *     group_type => COMPACT_STRING
 *
 * Adds `groupType` on each group (KIP-848). Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const groupSchema = flexibleObject([
  field('groupId', compactString),
  field('protocolType', compactString),
  field('groupState', compactString),
  field('groupType', compactString),
]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('groups', compactArray(groupSchema)),
]);

export const listGroupsResponseV5: ResponseDefinition<ListGroupsResponseV5Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await listGroupsResponseV4.parse(data);
    return data;
  },
};
