import { Decoder } from '../../../decoder';
import {
  compactArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int8,
  nullableFlexibleObject,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import {
  memberSchema,
  parseStreamsGroupDescribeResponse,
  topologySchema,
  type StreamsGroupDescribeGroupV0,
  type StreamsGroupDescribeMemberV0,
  type StreamsGroupDescribeResponseV0Body,
} from '../v0/response';

/** The same decoded shape as v0, which fills the topology description fields and `assignorName` with their defaults. */
export type StreamsGroupDescribeMemberV1 = StreamsGroupDescribeMemberV0;
export type StreamsGroupDescribeGroupV1 = StreamsGroupDescribeGroupV0;
export type StreamsGroupDescribeResponseV1Body = StreamsGroupDescribeResponseV0Body;

/**
 * StreamsGroupDescribe Response (Version: 1) adds `topology_description` (nullable),
 * `topology_description_status` (KIP-1331) and `assignor_name` (nullable, KIP-1357) after
 * `authorized_operations`.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const topologyDescriptionNodeSchema = flexibleObject([
  field('name', compactString),
  field('nodeType', int8),
  field('sourceTopics', compactArray(compactString)),
  field('sinkTopic', compactNullableString),
  field('stores', compactArray(compactString)),
  field('successors', compactArray(compactString)),
]);
const topologyDescriptionSchema = nullableFlexibleObject([
  field(
    'subtopologies',
    compactArray(
      flexibleObject([
        field('subtopologyId', compactString),
        field('nodes', compactArray(topologyDescriptionNodeSchema)),
      ]),
    ),
  ),
  field(
    'globalStores',
    compactArray(
      flexibleObject([
        field('source', topologyDescriptionNodeSchema),
        field('processor', topologyDescriptionNodeSchema),
      ]),
    ),
  ),
]);
const groupSchema = flexibleObject([
  field('errorCode', int16),
  field('errorMessage', compactNullableString),
  field('groupId', compactString),
  field('groupState', compactString),
  field('groupEpoch', int32),
  field('assignmentEpoch', int32),
  field('topology', topologySchema),
  field('members', compactArray(memberSchema)),
  field('authorizedOperations', int32),
  field('topologyDescription', topologyDescriptionSchema),
  field('topologyDescriptionStatus', int8),
  field('assignorName', compactNullableString),
]);
const responseSchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

export const streamsGroupDescribeResponseV1: ResponseDefinition<StreamsGroupDescribeResponseV1Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return { ...decoded, throttleTime: 0, clientSideThrottleTime: decoded.throttleTime };
  },
  parse: async (data) => parseStreamsGroupDescribeResponse(data),
};
