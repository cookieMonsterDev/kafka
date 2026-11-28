import { createErrorFromCode, failure } from '../../error-codes';
import { compactNullableArray, type FieldCodec } from '../../schema';

/** First OffsetFetch version that addresses topics by id instead of name (KIP-848, KIP-516). */
export const OFFSET_FETCH_TOPIC_ID_MIN_VERSION = 10;

export interface OffsetFetchTopicOptions {
  topic: string;
  /** The topic's id, which v10+ sends in place of its name. */
  topicId?: Buffer | undefined;
  partitions: { partition: number }[];
}

export interface OffsetFetchGroupOptions {
  groupId: string;
  /**
   * v9+: the id of the consumer protocol member sending the request (KIP-848), so the coordinator
   * can reject a member with a stale epoch. `null`, the default, fetches without that check.
   */
  memberId?: string | null;
  /** v9+: that member's epoch; `-1`, the default, without a member id. */
  memberEpoch?: number;
  /** Empty fetches every topic the group has committed offsets for. */
  topics: OffsetFetchTopicOptions[];
}

/** Fills each group's v9+ member fields with their defaults: no member id, epoch `-1`. */
export function withMemberDefaults(
  groups: readonly OffsetFetchGroupOptions[],
): (OffsetFetchGroupOptions & { memberId: string | null; memberEpoch: number })[] {
  return groups.map(({ memberId = null, memberEpoch = -1, ...group }) => ({ ...group, memberId, memberEpoch }));
}

/**
 * Compact array that writes `[]` as wire-null and reads wire-null as `[]`: an empty `topics` list
 * means "every topic the group has committed offsets for".
 */
export function emptyAsNullCompactArray<T>(element: FieldCodec<T>): FieldCodec<T[]> {
  const nullable = compactNullableArray(element);
  return {
    write(encoder, values) {
      nullable.write(encoder, values.length === 0 ? null : values);
    },
    read(decoder) {
      return nullable.read(decoder) ?? [];
    },
  };
}

export interface OffsetFetchResponseShape {
  responses: readonly { partitions: readonly { errorCode: number }[] }[];
}

export function checkOffsetFetchPartitionErrors(data: OffsetFetchResponseShape): void {
  for (const response of data.responses) {
    const partitionWithError = response.partitions.find((partition) => failure(partition.errorCode));
    if (partitionWithError) throw createErrorFromCode(partitionWithError.errorCode);
  }
}

export interface OffsetFetchGroupsResponseShape {
  groups: readonly { errorCode: number; topics: readonly { partitions: readonly { errorCode: number }[] }[] }[];
}

/** v8+: throws the first group-level error, then the first partition-level one. */
export function checkOffsetFetchGroupErrors(data: OffsetFetchGroupsResponseShape): void {
  for (const group of data.groups) {
    if (failure(group.errorCode)) throw createErrorFromCode(group.errorCode);
    checkOffsetFetchPartitionErrors({ responses: group.topics });
  }
}
