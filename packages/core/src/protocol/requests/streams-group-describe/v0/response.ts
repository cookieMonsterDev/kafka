import { Decoder } from '../../../decoder';
import { createErrorFromCode, failure } from '../../../error-codes';
import {
  boolean,
  compactArray,
  compactNullableArray,
  compactNullableString,
  compactString,
  field,
  flexibleObject,
  int16,
  int32,
  int64,
  nullableFlexibleObject,
  uint16,
} from '../../../schema';
import type { ResponseDefinition } from '../../../schema';
import { STREAMS_TOPOLOGY_DESCRIPTION_STATUSES } from '../../../enums/streams-topology-description-statuses';

export interface StreamsGroupDescribeKeyValue {
  key: string;
  value: string;
}

/** An internal topic of a subtopology: a state changelog topic or a repartition source topic. */
export interface StreamsGroupDescribeTopicInfo {
  name: string;
  /** `0` when no partition count is enforced; always `0` for a changelog topic. */
  partitions: number;
  /** `0` when the broker's default replication factor applies. */
  replicationFactor: number;
  topicConfigs: StreamsGroupDescribeKeyValue[];
}

export interface StreamsGroupDescribeSubtopology {
  subtopologyId: string;
  sourceTopics: string[];
  repartitionSinkTopics: string[];
  stateChangelogTopics: StreamsGroupDescribeTopicInfo[];
  repartitionSourceTopics: StreamsGroupDescribeTopicInfo[];
}

/** The topology the group coordinator has initialized for the group. */
export interface StreamsGroupDescribeTopology {
  epoch: number;
  /** `null` while the group is uninitialized or its source topics are missing or wrongly partitioned. */
  subtopologies: StreamsGroupDescribeSubtopology[] | null;
}

export interface StreamsGroupDescribeEndpoint {
  host: string;
  port: number;
}

export interface StreamsGroupDescribeTaskOffset {
  subtopologyId: string;
  partition: number;
  offset: bigint;
}

/** The tasks of one subtopology, named by the partitions of its input topics. */
export interface StreamsGroupDescribeTaskIds {
  subtopologyId: string;
  partitions: number[];
}

export interface StreamsGroupDescribeAssignment {
  activeTasks: StreamsGroupDescribeTaskIds[];
  standbyTasks: StreamsGroupDescribeTaskIds[];
  warmupTasks: StreamsGroupDescribeTaskIds[];
}

export interface StreamsGroupDescribeMemberV0 {
  memberId: string;
  memberEpoch: number;
  instanceId: string | null;
  rackId: string | null;
  clientId: string;
  clientHost: string;
  /** The epoch of the topology the member runs. */
  topologyEpoch: number;
  /** The Kafka Streams instance the member belongs to; one instance can run several members. */
  processId: string;
  /** The member's interactive queries endpoint, or `null` when it has none. */
  userEndpoint: StreamsGroupDescribeEndpoint | null;
  clientTags: StreamsGroupDescribeKeyValue[];
  /** Cumulative changelog offsets per task. */
  taskOffsets: StreamsGroupDescribeTaskOffset[];
  /** Cumulative changelog end offsets per task. */
  taskEndOffsets: StreamsGroupDescribeTaskOffset[];
  assignment: StreamsGroupDescribeAssignment;
  targetAssignment: StreamsGroupDescribeAssignment;
  /** `true` for a classic member that has not moved to the streams protocol yet. */
  isClassic: boolean;
}

export interface StreamsTopologyDescriptionNode {
  name: string;
  /** `1` source, `2` processor, `3` sink. */
  nodeType: number;
  /** Set only on a source node; empty when its topics are chosen at runtime. */
  sourceTopics: string[];
  /** Set only on a sink node; `null` when its topic is chosen at runtime. */
  sinkTopic: string | null;
  /** Set only on a processor node. */
  stores: string[];
  successors: string[];
}

export interface StreamsTopologyDescriptionSubtopology {
  subtopologyId: string;
  nodes: StreamsTopologyDescriptionNode[];
}

export interface StreamsTopologyDescriptionGlobalStore {
  source: StreamsTopologyDescriptionNode;
  processor: StreamsTopologyDescriptionNode;
}

/** The full processing graph of a streams group's topology (KIP-1331). */
export interface StreamsTopologyDescription {
  subtopologies: StreamsTopologyDescriptionSubtopology[];
  globalStores: StreamsTopologyDescriptionGlobalStore[];
}

export interface StreamsGroupDescribeGroupV0 {
  errorCode: number;
  errorMessage: string | null;
  groupId: string;
  groupState: string;
  groupEpoch: number;
  assignmentEpoch: number;
  /** `null` after a describe error. */
  topology: StreamsGroupDescribeTopology | null;
  members: StreamsGroupDescribeMemberV0[];
  authorizedOperations: number;
  /**
   * Set only when `topologyDescriptionStatus` is `AVAILABLE`. Version 0 has no such field on the
   * wire, so it decodes as `null`, the field's default in Kafka's `StreamsGroupDescribeResponse.json`.
   */
  topologyDescription: StreamsTopologyDescription | null;
  /** One of `StreamsTopologyDescriptionStatuses`; version 0 decodes as `NOT_REQUESTED` (0). */
  topologyDescriptionStatus: number;
  /**
   * The task assignor the group coordinator uses for the next assignment (KIP-1357). `null` after
   * a describe error, and on version 0, which does not carry it.
   */
  assignorName: string | null;
}

export interface StreamsGroupDescribeResponseV0Body {
  throttleTime: number;
  clientSideThrottleTime: number;
  groups: StreamsGroupDescribeGroupV0[];
}

/**
 * StreamsGroupDescribe Response (Version: 0) => throttle_time_ms [groups] TAG_BUFFER
 *
 * Topology, user endpoint and every nested struct are flexible structs; topology and user
 * endpoint are nullable. Quota timing follows KIP-219.
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
const keyValueSchema = flexibleObject([field('key', compactString), field('value', compactString)]);
const topicInfoSchema = flexibleObject([
  field('name', compactString),
  field('partitions', int32),
  field('replicationFactor', int16),
  field('topicConfigs', compactArray(keyValueSchema)),
]);
const subtopologySchema = flexibleObject([
  field('subtopologyId', compactString),
  field('sourceTopics', compactArray(compactString)),
  field('repartitionSinkTopics', compactArray(compactString)),
  field('stateChangelogTopics', compactArray(topicInfoSchema)),
  field('repartitionSourceTopics', compactArray(topicInfoSchema)),
]);
export const topologySchema = nullableFlexibleObject([
  field('epoch', int32),
  field('subtopologies', compactNullableArray(subtopologySchema)),
]);
const endpointSchema = nullableFlexibleObject([field('host', compactString), field('port', uint16)]);
const taskOffsetSchema = flexibleObject([
  field('subtopologyId', compactString),
  field('partition', int32),
  field('offset', int64),
]);
const taskIdsSchema = flexibleObject([field('subtopologyId', compactString), field('partitions', compactArray(int32))]);
const assignmentSchema = flexibleObject([
  field('activeTasks', compactArray(taskIdsSchema)),
  field('standbyTasks', compactArray(taskIdsSchema)),
  field('warmupTasks', compactArray(taskIdsSchema)),
]);
export const memberSchema = flexibleObject([
  field('memberId', compactString),
  field('memberEpoch', int32),
  field('instanceId', compactNullableString),
  field('rackId', compactNullableString),
  field('clientId', compactString),
  field('clientHost', compactString),
  field('topologyEpoch', int32),
  field('processId', compactString),
  field('userEndpoint', endpointSchema),
  field('clientTags', compactArray(keyValueSchema)),
  field('taskOffsets', compactArray(taskOffsetSchema)),
  field('taskEndOffsets', compactArray(taskOffsetSchema)),
  field('assignment', assignmentSchema),
  field('targetAssignment', assignmentSchema),
  field('isClassic', boolean),
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
]);
const responseSchema = flexibleObject([field('throttleTime', int32), field('groups', compactArray(groupSchema))]);

/** Rejects the first group with an error, with the broker's message when it sends one. */
export function parseStreamsGroupDescribeResponse<T extends StreamsGroupDescribeResponseV0Body>(data: T): T {
  const failed = data.groups.find(({ errorCode }) => failure(errorCode));
  if (failed) throw createErrorFromCode(failed.errorCode, { message: failed.errorMessage });
  return data;
}

export const streamsGroupDescribeResponseV0: ResponseDefinition<StreamsGroupDescribeResponseV0Body> = {
  decode: async (rawData) => {
    const decoded = responseSchema.read(new Decoder(rawData));
    return {
      throttleTime: 0,
      clientSideThrottleTime: decoded.throttleTime,
      groups: decoded.groups.map((group) => ({
        ...group,
        topologyDescription: null,
        topologyDescriptionStatus: STREAMS_TOPOLOGY_DESCRIPTION_STATUSES.NOT_REQUESTED,
        assignorName: null,
      })),
    };
  },
  parse: async (data) => parseStreamsGroupDescribeResponse(data),
};
