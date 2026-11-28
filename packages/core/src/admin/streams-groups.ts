import { KafkaNonRetriableError } from '../errors';
import type { AdminContext } from './helpers';
import { assertValidGroupIds, describeGroupsViaCoordinators, formatUnknown } from './helpers';
import type { DescribeStreamsGroupsOptions, StreamsGroupDescription } from './types';
import { StreamsGroupDescribe } from '../protocol/requests/lazy-families';

export interface StreamsGroupsApi {
  describeStreamsGroups: (
    groupIds: string[],
    options?: DescribeStreamsGroupsOptions,
  ) => Promise<{ groups: StreamsGroupDescription[] }>;
}

export function createStreamsGroupsApi({ cluster, retry }: AdminContext): StreamsGroupsApi {
  const describeStreamsGroups = async (
    groupIds: string[],
    { includeTopologyDescription = false }: DescribeStreamsGroupsOptions = {},
  ): Promise<{ groups: StreamsGroupDescription[] }> => {
    assertValidGroupIds(groupIds);
    if (typeof includeTopologyDescription !== 'boolean') {
      throw new KafkaNonRetriableError(
        `Invalid includeTopologyDescription ${formatUnknown(includeTopologyDescription)}`,
      );
    }
    if (groupIds.length === 0) return { groups: [] };

    return {
      groups: await describeGroupsViaCoordinators({
        cluster,
        retry,
        groupIds,
        describe: (coordinator, ids) =>
          coordinator.send(StreamsGroupDescribe, { groupIds: ids, includeTopologyDescription }),
      }),
    };
  };

  return { describeStreamsGroups };
}
