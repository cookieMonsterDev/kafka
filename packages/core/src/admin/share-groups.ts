import { KafkaNonRetriableError } from '../errors';
import type { ShareGroupDescribeGroupV1 } from '../protocol/requests/share-group-describe/v1/response';
import type { AlterShareGroupOffsetsResponseV0Body } from '../protocol/requests/alter-share-group-offsets/v0/response';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { DeleteShareGroupOffsetsResponseV0Body } from '../protocol/requests/delete-share-group-offsets/v0/response';
import type { DescribeShareGroupOffsetsResponseV1Body } from '../protocol/requests/describe-share-group-offsets/v1/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { deleteGroupsViaCoordinators, formatUnknown, protocolType } from './helpers';
import type {
  AlterShareGroupOffsetsOptions,
  DeleteShareGroupOffsetsOptions,
  DescribeShareGroupOffsetsOptions,
  ListShareGroupOffsetsOptions,
} from './types';

export interface ShareGroupsApi {
  describeShareGroups: (groupIds: string[]) => Promise<{ groups: ShareGroupDescribeGroupV1[] }>;
  listShareGroupOffsets: (
    options: ListShareGroupOffsetsOptions,
  ) => Promise<{ groups: DescribeShareGroupOffsetsResponseV1Body['groups'] }>;
  alterShareGroupOffsets: (
    options: AlterShareGroupOffsetsOptions,
  ) => Promise<{ responses: AlterShareGroupOffsetsResponseV0Body['responses'] }>;
  deleteShareGroupOffsets: (
    options: DeleteShareGroupOffsetsOptions,
  ) => Promise<{ responses: DeleteShareGroupOffsetsResponseV0Body['responses'] }>;
  deleteShareGroups: (groupIds: string[]) => Promise<DeleteGroupsResult[]>;
}

function assertNonEmptyGroupIds(groupIds: unknown): asserts groupIds is string[] {
  if (!Array.isArray(groupIds)) {
    throw new KafkaNonRetriableError(`Invalid groupIds array ${formatUnknown(groupIds)}`);
  }
  if (groupIds.some((groupId) => typeof groupId !== 'string' || groupId === '')) {
    throw new KafkaNonRetriableError('Group IDs must be non-empty strings');
  }
}

export function createShareGroupsApi({ cluster, logger, retry }: AdminContext): ShareGroupsApi {
  const describeShareGroups = async (groupIds: string[]): Promise<{ groups: ShareGroupDescribeGroupV1[] }> => {
    assertNonEmptyGroupIds(groupIds);
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
        const { groups } = await retrier(retry)(() => coordinator.shareGroupDescribe({ groupIds: ids }));
        return groups;
      }),
    );

    return { groups: responses.flat() };
  };

  const listShareGroupOffsets = async ({
    groups,
  }: ListShareGroupOffsetsOptions): Promise<{ groups: DescribeShareGroupOffsetsResponseV1Body['groups'] }> => {
    if (!Array.isArray(groups) || groups.length === 0) {
      throw new KafkaNonRetriableError(`Invalid groups array ${formatUnknown(groups)}`);
    }

    for (const { groupId } of groups) {
      if (typeof groupId !== 'string' || groupId === '') {
        throw new KafkaNonRetriableError('Each group must have a non-empty groupId');
      }
    }

    const coordinatorsForGroup = await Promise.all(
      groups.map(async (entry) => {
        const coordinator = await cluster.findGroupCoordinator({ groupId: entry.groupId });
        return { coordinator, entry };
      }),
    );

    const groupsByCoordinator = new Map<
      string,
      {
        coordinator: (typeof coordinatorsForGroup)[number]['coordinator'];
        groups: DescribeShareGroupOffsetsOptions['groups'];
      }
    >();
    for (const { coordinator, entry } of coordinatorsForGroup) {
      const key = String(coordinator.nodeId);
      const existing = groupsByCoordinator.get(key);
      if (existing) {
        existing.groups.push(entry);
      } else {
        groupsByCoordinator.set(key, { coordinator, groups: [entry] });
      }
    }

    const responses = await Promise.all(
      [...groupsByCoordinator.values()].map(async ({ coordinator, groups: grouped }) => {
        const { groups: resultGroups } = await retrier(retry)(() =>
          coordinator.describeShareGroupOffsets({
            groups: grouped.map(({ groupId, topics }) => ({
              groupId,
              topics:
                topics == null
                  ? null
                  : topics.map(({ topicName, partitions }) => ({
                      topicName,
                      partitions: partitions ?? [],
                    })),
            })),
          }),
        );
        return resultGroups;
      }),
    );

    return { groups: responses.flat() };
  };

  const alterShareGroupOffsets = async ({
    groupId,
    topics,
  }: AlterShareGroupOffsetsOptions): Promise<{ responses: AlterShareGroupOffsetsResponseV0Body['responses'] }> => {
    if (!groupId || typeof groupId !== 'string') {
      throw new KafkaNonRetriableError(`Invalid groupId ${formatUnknown(groupId)}`);
    }
    if (!Array.isArray(topics) || topics.length === 0) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        const { responses } = await coordinator.alterShareGroupOffsets({ groupId, topics });
        return { responses };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not alter share group offsets', {
            error: error instanceof Error ? error.message : String(error),
            groupId,
            retryCount,
            retryTime,
          });
          throw error;
        }
        bail(error as Error);
        return { responses: [] };
      }
    });
  };

  const deleteShareGroupOffsets = async ({
    groupId,
    topics,
  }: DeleteShareGroupOffsetsOptions): Promise<{ responses: DeleteShareGroupOffsetsResponseV0Body['responses'] }> => {
    if (!groupId || typeof groupId !== 'string') {
      throw new KafkaNonRetriableError(`Invalid groupId ${formatUnknown(groupId)}`);
    }
    if (!Array.isArray(topics) || topics.length === 0) {
      throw new KafkaNonRetriableError(`Invalid topics array ${formatUnknown(topics)}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const coordinator = await cluster.findGroupCoordinator({ groupId });
        const { responses } = await coordinator.deleteShareGroupOffsets({
          groupId,
          topics,
        });
        return { responses };
      } catch (error) {
        const type = protocolType(error);
        if (type === 'GROUP_COORDINATOR_NOT_AVAILABLE' || type === 'NOT_COORDINATOR_FOR_GROUP') {
          logger.warn('Could not delete share group offsets', {
            error: error instanceof Error ? error.message : String(error),
            groupId,
            retryCount,
            retryTime,
          });
          throw error;
        }
        bail(error as Error);
        return { responses: [] };
      }
    });
  };

  const deleteShareGroups = async (groupIds: string[]): Promise<DeleteGroupsResult[]> => {
    assertNonEmptyGroupIds(groupIds);

    return deleteGroupsViaCoordinators({
      cluster,
      logger,
      retry,
      groupIds,
      errorLabel: 'Error in DeleteShareGroups',
      logMessage: 'Could not delete share groups',
    });
  };

  return {
    describeShareGroups,
    listShareGroupOffsets,
    alterShareGroupOffsets,
    deleteShareGroupOffsets,
    deleteShareGroups,
  };
}
