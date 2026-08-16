import { Decoder } from '../../../decoder';
import { compactArray, compactString, field, flexibleObject, int16, int32 } from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { listGroupsResponseV2, type ListGroupsResponseV2Body } from '../v2/response';

export type ListGroupsResponseV3Body = ListGroupsResponseV2Body;

/**
 * ListGroups Response (Version: 3) => throttle_time_ms error_code [groups] TAG_BUFFER
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   groups => group_id protocol_type TAG_BUFFER
 *     group_id => COMPACT_STRING
 *     protocol_type => COMPACT_STRING
 *
 * First flexible version (KIP-482). Same fields as v2. Quota timing follows v2 (KIP-219).
 *
 * @see https://kafka.apache.org/43/design/protocol/
 */
const groupSchema = flexibleObject([field('groupId', compactString), field('protocolType', compactString)]);
const bodySchema = flexibleObject([
  field('throttleTime', int32),
  field('errorCode', int16),
  field('groups', compactArray(groupSchema)),
]);

export const listGroupsResponseV3: ResponseDefinition<ListGroupsResponseV3Body> = {
  decode: async (rawData) => {
    const decoded = bodySchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => {
    await listGroupsResponseV2.parse(data);
    return data;
  },
};
