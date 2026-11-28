import { isUsableTopicId } from '../topic-ids';
import { defineRequestFamily, highestVersionBelow, type VersionFactories, type VersionResponse } from '../index';
import { KAFKA_APIS } from '../kafka-apis';
import { OFFSET_FETCH_TOPIC_ID_MIN_VERSION, withMemberDefaults, type OffsetFetchGroupOptions } from './shared';
import { offsetFetchRequestV1 } from './v1/request';
import { offsetFetchResponseV1 } from './v1/response';
import { offsetFetchRequestV2 } from './v2/request';
import { offsetFetchResponseV2 } from './v2/response';
import { offsetFetchRequestV3 } from './v3/request';
import { offsetFetchResponseV3 } from './v3/response';
import { offsetFetchRequestV4 } from './v4/request';
import { offsetFetchResponseV4 } from './v4/response';
import { offsetFetchRequestV5 } from './v5/request';
import { offsetFetchResponseV5 } from './v5/response';
import { offsetFetchRequestV6 } from './v6/request';
import { offsetFetchResponseV6 } from './v6/response';
import { offsetFetchRequestV7 } from './v7/request';
import { offsetFetchResponseV7 } from './v7/response';
import { offsetFetchRequestV8 } from './v8/request';
import { offsetFetchResponseV8 } from './v8/response';
import { offsetFetchRequestV9 } from './v9/request';
import { offsetFetchResponseV9 } from './v9/response';
import { offsetFetchRequestV10 } from './v10/request';
import { offsetFetchResponseV10 } from './v10/response';

export type { OffsetFetchGroupOptions, OffsetFetchTopicOptions } from './shared';

export interface OffsetFetchOptions {
  /** Versions before 8 fetch exactly one group; v8 (KIP-709) fetches several in one request. */
  groups: readonly OffsetFetchGroupOptions[];
  requireStable?: boolean;
}

/** One partition's committed offset; `leaderEpoch` is `-1` where unknown or before v5. */
export interface OffsetFetchPartitionResult {
  partition: number;
  offset: bigint;
  leaderEpoch: number;
  metadata: string | null;
}

export interface OffsetFetchTopicResult {
  topic: string;
  partitions: OffsetFetchPartitionResult[];
}

/** An OffsetFetch response in one shape for every version: the committed offsets of each group. */
export interface OffsetFetchResponse {
  groups: { groupId: string; topics: OffsetFetchTopicResult[] }[];
}

/** The one group a version before 8 fetches. */
function singleGroup({ groups }: OffsetFetchOptions): OffsetFetchGroupOptions {
  const [group] = groups;
  if (group === undefined || groups.length > 1) {
    throw new Error('Invariant violated: OffsetFetch before v8 fetches exactly one group');
  }
  return group;
}

const VERSIONS = {
  1: (options) => ({ request: offsetFetchRequestV1(singleGroup(options)), response: offsetFetchResponseV1 }),
  2: (options) => ({ request: offsetFetchRequestV2(singleGroup(options)), response: offsetFetchResponseV2 }),
  3: (options) => ({ request: offsetFetchRequestV3(singleGroup(options)), response: offsetFetchResponseV3 }),
  4: (options) => ({ request: offsetFetchRequestV4(singleGroup(options)), response: offsetFetchResponseV4 }),
  5: (options) => ({ request: offsetFetchRequestV5(singleGroup(options)), response: offsetFetchResponseV5 }),
  6: (options) => ({ request: offsetFetchRequestV6(singleGroup(options)), response: offsetFetchResponseV6 }),
  7: (options) => ({
    request: offsetFetchRequestV7({ ...singleGroup(options), requireStable: options.requireStable ?? false }),
    response: offsetFetchResponseV7,
  }),
  8: ({ groups, requireStable = false }) => ({
    request: offsetFetchRequestV8({ groups: [...groups], requireStable }),
    response: offsetFetchResponseV8,
  }),
  9: ({ groups, requireStable = false }) => ({
    request: offsetFetchRequestV9({ groups: withMemberDefaults(groups), requireStable }),
    response: offsetFetchResponseV9,
  }),
  10: ({ groups, requireStable = false }) => ({
    request: offsetFetchRequestV10({ groups: withMemberDefaults(groups), requireStable }),
    response: offsetFetchResponseV10(groups),
  }),
} satisfies VersionFactories<OffsetFetchOptions>;

const OFFSET_FETCH_VERSIONS: readonly number[] = Object.keys(VERSIONS).map(Number);

/**
 * v10 needs a topic id for every requested topic. A group that asks for all of its topics has
 * none to give, and v10 would answer it with ids only, so it stays on v9 too.
 */
function hasUsableTopicIds({ groups }: OffsetFetchOptions): boolean {
  return groups.every(({ topics }) => topics.length > 0 && topics.every(({ topicId }) => isUsableTopicId(topicId)));
}

function toTopicResult({
  topic,
  partitions,
}: {
  topic: string;
  partitions: readonly { partition: number; offset: bigint; leaderEpoch?: number; metadata: string | null }[];
}): OffsetFetchTopicResult {
  return {
    topic,
    partitions: partitions.map(({ partition, offset, leaderEpoch = -1, metadata }) => ({
      partition,
      offset,
      leaderEpoch,
      metadata,
    })),
  };
}

/**
 * v1–v7 answer for their one group without naming it; v8+ answer per group. v10 restores topic
 * names in its own `decode`.
 */
function normalizeOffsetFetch(
  body: VersionResponse<typeof VERSIONS>,
  options: OffsetFetchOptions,
): OffsetFetchResponse {
  if ('groups' in body) {
    return { groups: body.groups.map(({ groupId, topics }) => ({ groupId, topics: topics.map(toTopicResult) })) };
  }
  return { groups: [{ groupId: singleGroup(options).groupId, topics: body.responses.map(toTopicResult) }] };
}

/**
 * v8 fetches several groups (KIP-709), v9 adds the consumer protocol's member id and epoch
 * (KIP-848), and v10 addresses topics by id. Lookup picks the highest overlapping version; a
 * request that cannot name every topic by id stays on the highest name-based one (v9).
 *
 * @see https://kafka.apache.org/44/design/protocol/
 */
export const OffsetFetch = defineRequestFamily<typeof VERSIONS, OffsetFetchResponse>(KAFKA_APIS.OffsetFetch, VERSIONS, {
  preferVersion: (options, version, advertised) =>
    version < OFFSET_FETCH_TOPIC_ID_MIN_VERSION || hasUsableTopicIds(options)
      ? version
      : highestVersionBelow(
          KAFKA_APIS.OffsetFetch,
          OFFSET_FETCH_VERSIONS,
          OFFSET_FETCH_TOPIC_ID_MIN_VERSION,
          advertised,
        ),
  parseResponse: async (body, options, parse) =>
    normalizeOffsetFetch((await parse(body)) as VersionResponse<typeof VERSIONS>, options),
});

/** The committed offsets `response` holds for `groupId`; none when the response leaves the group out. */
export function committedOffsetsOf(response: OffsetFetchResponse, groupId: string): OffsetFetchTopicResult[] {
  return response.groups.find((group) => group.groupId === groupId)?.topics ?? [];
}
