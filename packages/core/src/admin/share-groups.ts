import { KafkaNonRetriableError } from '../errors';
import type { ShareGroupDescribeGroupV1 } from '../protocol/requests/share-group-describe/v1/response';
import type { AlterShareGroupOffsetsResponseV0Body } from '../protocol/requests/alter-share-group-offsets/v0/response';
import type { DeleteGroupsResult } from '../protocol/requests/delete-groups/v0/response';
import type { DeleteShareGroupOffsetsResponseV0Body } from '../protocol/requests/delete-share-group-offsets/v0/response';
import type { DescribeShareGroupOffsetsResponseV1Body } from '../protocol/requests/describe-share-group-offsets/v1/response';
import { coordinatorTarget, nodeTarget } from '../cluster/run';
import { retrier as createRetrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { deleteGroupsViaCoordinators, describeGroupsViaCoordinators, formatUnknown, groupByBroker } from './helpers';
import type {
  AlterShareGroupOffsetsOptions,
  DeleteShareGroupOffsetsOptions,
  ListShareGroupOffsetsOptions,
} from './types';
import {
  AlterShareGroupOffsets,
  DeleteShareGroupOffsets,
  DescribeShareGroupOffsets,
  ShareGroupDescribe,
} from '../protocol/requests/lazy-families';

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

export function createShareGroupsApi({ cluster, retry }: AdminContext): ShareGroupsApi {
  const retrier = createRetrier(retry);

  const describeShareGroups = async (groupIds: string[]): Promise<{ groups: ShareGroupDescribeGroupV1[] }> => {
    assertNonEmptyGroupIds(groupIds);
    if (groupIds.length === 0) return { groups: [] };

    return {
      groups: await describeGroupsViaCoordinators({
        cluster,
        retry,
        groupIds,
        describe: (coordinator, ids) => coordinator.send(ShareGroupDescribe, { groupIds: ids }),
      }),
    };
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

    // Coordinators are looked up once, then each coordinator's request is retried on its own.
    const coordinators = await Promise.all(groups.map(({ groupId }) => cluster.findGroupCoordinator({ groupId })));
    const responses = await Promise.all(
      [...groupByBroker(groups, coordinators)].map(([coordinator, grouped]) =>
        cluster.run(
          nodeTarget(String(coordinator.nodeId)),
          async (broker) => {
            const { groups: resultGroups } = await broker.send(DescribeShareGroupOffsets, {
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
            });
            return resultGroups;
          },
          { retrier, retryRetriable: true },
        ),
      ),
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

    return cluster.run(
      coordinatorTarget(groupId),
      async (coordinator) => {
        const { responses } = await coordinator.send(AlterShareGroupOffsets, { groupId, topics });
        return { responses };
      },
      { retrier, refreshMetadata: true, description: 'Could not alter share group offsets' },
    );
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

    return cluster.run(
      coordinatorTarget(groupId),
      async (coordinator) => {
        const { responses } = await coordinator.send(DeleteShareGroupOffsets, { groupId, topics });
        return { responses };
      },
      { retrier, refreshMetadata: true, description: 'Could not delete share group offsets' },
    );
  };

  const deleteShareGroups = async (groupIds: string[]): Promise<DeleteGroupsResult[]> => {
    assertNonEmptyGroupIds(groupIds);

    return deleteGroupsViaCoordinators({
      cluster,
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
