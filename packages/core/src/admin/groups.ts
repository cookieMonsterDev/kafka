import { KafkaNonRetriableError } from '../errors';
import type { ConsumerGroupDescribeGroupV1 } from '../protocol/requests/consumer-group-describe/v1/response';
import type { DescribeGroupsResponseV2Body } from '../protocol/requests/describe-groups/v2/response';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { ListGroupsResponseV2Body } from '../protocol/requests/list-groups/v2/response';
import type { OffsetDeleteResponseV0Body } from '../protocol/requests/offset-delete/v0/response';
import type { LeaveGroupMember } from '../protocol/requests/leave-group/index';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { deleteGroupsViaCoordinators, protocolType, formatUnknown } from './helpers';
import type { RemoveMembersFromConsumerGroupOptions, RemoveMembersFromConsumerGroupResult } from './types';
import type { TopicPartitions } from './types';

export interface GroupsApi {
  listGroups: () => Promise<{ groups: ListGroupsResponseV2Body['groups'] }>;
  describeGroups: (groupIds: string[]) => Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }>;
  describeClassicGroups: (groupIds: string[]) => Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }>;
  describeConsumerGroups: (groupIds: string[]) => Promise<{ groups: ConsumerGroupDescribeGroupV1[] }>;
  deleteGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>;
  deleteGroupOffsets: (options: {
    groupId: string;
    topics: TopicPartitions[];
  }) => Promise<{ topics: OffsetDeleteResponseV0Body['topics'] }>;
  removeMembersFromConsumerGroup: (
    options: RemoveMembersFromConsumerGroupOptions,
  ) => Promise<{ members: RemoveMembersFromConsumerGroupResult[] }>;
}

export function createGroupsApi({ cluster, logger, retry }: AdminContext): GroupsApi {
  const listGroups = async (): Promise<{ groups: ListGroupsResponseV2Body['groups'] }> => {
    await cluster.refreshMetadata();

    const responses = await Promise.all(
      Object.keys(cluster.brokerPool.brokers).map(async (nodeId) => {
        const broker = await cluster.findBroker({ nodeId });
        return broker.listGroups();
      }),
    );

    return { groups: responses.flatMap((response) => response.groups) };
  };

  const describeGroups = async (groupIds: string[]): Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }> => {
    const coordinatorsForGroup = await Promise.all(
      groupIds.map(async (groupId) => {
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        return { coordinator, groupId };
      }),
    );

    const groupsByCoordinator = new Map<
      string,
      { coordinator: (typeof coordinatorsForGroup)[number]['coordinator']; groupIds: string[] }
    >();
    for (const { coordinator, groupId } of coordinatorsForGroup) {
      const key = String(coordinator.nodeId);
      const existing = groupsByCoordinator.get(key);
      if (existing) {
        existing.groupIds.push(groupId);
      } else {
        groupsByCoordinator.set(key, { coordinator, groupIds: [groupId] });
      }
    }

    const responses = await Promise.all(
      [...groupsByCoordinator.values()].map(async ({ coordinator, groupIds: ids }) => {
        const { groups } = await retrier(retry)(() => coordinator.describeGroups({ groupIds: ids }));
        return groups;
      }),
    );

    return { groups: responses.flat() };
  };

  const describeClassicGroups = async (
    groupIds: string[],
  ): Promise<{ groups: DescribeGroupsResponseV2Body['groups'] }> => describeGroups(groupIds);

  const describeConsumerGroups = async (groupIds: string[]): Promise<{ groups: ConsumerGroupDescribeGroupV1[] }> => {
    if (!Array.isArray(groupIds)) {
      throw new KafkaNonRetriableError(`Invalid groupIds array ${formatUnknown(groupIds)}`);
    }
    if (groupIds.some((groupId) => typeof groupId !== 'string' || groupId === '')) {
      throw new KafkaNonRetriableError('Group IDs must be non-empty strings');
    }
    if (groupIds.length === 0) return { groups: [] };

    const coordinatorsForGroup = await Promise.all(
      groupIds.map(async (groupId) => {
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        return { coordinator, groupId };
      }),
    );

    const groupsByCoordinator = new Map<
      string,
      { coordinator: (typeof coordinatorsForGroup)[number]['coordinator']; groupIds: string[] }
    >();
    for (const { coordinator, groupId } of coordinatorsForGroup) {
      const key = String(coordinator.nodeId);
      const existing = groupsByCoordinator.get(key);
      if (existing) {
        existing.groupIds.push(groupId);
      } else {
        groupsByCoordinator.set(key, { coordinator, groupIds: [groupId] });
      }
    }

    const responses = await Promise.all(
      [...groupsByCoordinator.values()].map(async ({ coordinator, groupIds: ids }) => {
        const { groups } = await retrier(retry)(() => coordinator.consumerGroupDescribe({ groupIds: ids }));
        return groups;
      }),
    );

    return { groups: responses.flat() };
  };

  const deleteGroups = async (groupIds: string[]): Promise<DeleteGroupsResult[]> => {
    if (!groupIds || !Array.isArray(groupIds)) {
      throw new KafkaNonRetriableError(`Invalid groupIds array ${formatUnknown(groupIds)}`);
    }

    const invalidGroupId = groupIds.some((groupId) => typeof groupId !== 'string');
    if (invalidGroupId) {
      throw new KafkaNonRetriableError(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`);
    }

    return deleteGroupsViaCoordinators({
      cluster,
      logger,
      retry,
      groupIds,
      errorLabel: 'Error in DeleteGroups',
      logMessage: 'Could not delete groups',
    });
  };

  const deleteGroupOffsets = async ({
    groupId,
    topics,
  }: {
    groupId: string;
    topics: TopicPartitions[];
  }): Promise<{ topics: OffsetDeleteResponseV0Body['topics'] }> => {
    if (!groupId || typeof groupId !== 'string') {
      throw new KafkaNonRetriableError(`Invalid groupId ${formatUnknown(groupId)}`);
    }

    if (!topics || !Array.isArray(topics)) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    if (topics.filter(({ topic }) => typeof topic !== 'string').length > 0) {
      throw new KafkaNonRetriableError('Invalid topics array, the topic names have to be a valid string');
    }

    for (const { topic, partitions } of topics) {
      if (!partitions || !Array.isArray(partitions)) {
        throw new KafkaNonRetriableError(`Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}`);
      }

      if (partitions.filter((partition) => typeof partition !== 'number' || partition < 0).length >= 1) {
        throw new KafkaNonRetriableError(
          `Invalid partition array: ${formatUnknown(partitions)} for topic: ${topic}. The partition indices have to be a valid number greater than 0.`,
        );
      }
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        const response = await coordinator.offsetDelete({ groupId, topics });
        return { topics: response.topics };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not delete group offsets', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { topics: [] };
      }
    });
  };

  const removeMembersFromConsumerGroup = async ({
    groupId,
    members,
  }: RemoveMembersFromConsumerGroupOptions): Promise<{ members: RemoveMembersFromConsumerGroupResult[] }> => {
    if (!groupId || typeof groupId !== 'string') {
      throw new KafkaNonRetriableError(`Invalid groupId ${formatUnknown(groupId)}`);
    }
    if (!Array.isArray(members) || members.length === 0) {
      throw new KafkaNonRetriableError(`Invalid members array ${formatUnknown(members)}`);
    }
    for (const member of members) {
      if (typeof member.memberId !== 'string' || member.memberId.length === 0) {
        throw new KafkaNonRetriableError('Each member must have a non-empty memberId');
      }
      if (
        member.groupInstanceId !== undefined &&
        member.groupInstanceId !== null &&
        typeof member.groupInstanceId !== 'string'
      ) {
        throw new KafkaNonRetriableError(`Invalid groupInstanceId ${formatUnknown(member.groupInstanceId)}`);
      }
    }

    const leaveMembers: LeaveGroupMember[] = members.map(({ memberId, groupInstanceId, reason }) => ({
      memberId,
      groupInstanceId: groupInstanceId ?? null,
      reason: reason ?? null,
    }));

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        const { members: responseMembers } = await coordinator.leaveGroupMembers({ groupId, members: leaveMembers });
        return {
          members: responseMembers.map(({ memberId, groupInstanceId, errorCode }) => ({
            memberId,
            groupInstanceId,
            errorCode,
          })),
        };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not remove members from consumer group', {
            error: error instanceof Error ? error.message : String(error),
            groupId,
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { members: [] };
      }
    });
  };

  return {
    listGroups,
    describeGroups,
    describeClassicGroups,
    describeConsumerGroups,
    deleteGroups,
    deleteGroupOffsets,
    removeMembersFromConsumerGroup,
  };
}
