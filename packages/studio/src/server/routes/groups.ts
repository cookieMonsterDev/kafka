import {
  deleteGroupOffsetsRequestSchema,
  removeGroupMembersRequestSchema,
  resetGroupOffsetsRequestSchema,
  type DeleteGroupOffsetsResponse,
  type GroupDetailResponse,
  type GroupListResponse,
  type GroupMember,
  type GroupOffsetResetTarget,
  type GroupPartitionLag,
  type RemoveGroupMembersResponse,
  type ResetGroupOffsetsResponse,
  type ShareGroupDetailResponse,
  type ShareGroupListResponse,
} from '../../shared/contracts/group';
import { sendError, sendJson } from '../create-server';
import type { AdminPool, PooledAdmin } from '../kafka/admin-pool';
import { readJsonBody } from '../json';
import { requireParam, type Router } from '../router';

export interface GroupsRouteContext {
  readonly pool: AdminPool;
  getActiveProfile(): string | null;
}

/** Classic and KIP-848 consumer groups both report this `protocolType`; KIP-932 share groups report `share`. */
const CONSUMER_PROTOCOL_TYPE = 'consumer';
const SHARE_PROTOCOL_TYPE = 'share';

/**
 * The classic `DescribeGroups` API (what most consumers still join with — `kafka-consumer-groups.sh`
 * and this repo's own `kafka group describe` both use it) reports members as raw, assignor-specific
 * `memberAssignment` bytes with no public decoder in this codebase, so — matching the CLI's own
 * documented choice in `packages/cli/src/commands/group/describe.ts` — only the identifying fields
 * survive here. This is not a KIP-848 `ConsumerGroupDescribe` group, so there is no structured
 * per-member assignment to report; partition lag below is computed independently of it.
 */
function toGroupMember(member: { memberId: string; clientId: string; clientHost: string }): GroupMember {
  return {
    memberId: member.memberId,
    instanceId: null,
    clientId: member.clientId,
    clientHost: member.clientHost,
    assignedTopicPartitions: [],
  };
}

/**
 * A group's own describe response has no per-partition consumption telemetry — the studio derives
 * lag itself from two real reads: the group's own committed offsets (`fetchOffsets` with no topic
 * filter, which the OffsetFetch protocol treats as "every topic this group has committed to" — not
 * every topic on the cluster) and each of those topics' current high watermark (`fetchTopicOffsets`).
 */
async function computePartitionLag(admin: PooledAdmin, groupId: string): Promise<GroupPartitionLag[]> {
  const committedByTopic = await admin.fetchOffsets({ groupId });
  const topicList = committedByTopic.map((entry) => entry.topic);
  if (topicList.length === 0) return [];

  const endOffsetsByTopic = await Promise.all(
    topicList.map(async (topic) => ({ topic, offsets: await admin.fetchTopicOffsets(topic) })),
  );

  const endOffsetByTopicPartition = new Map<string, bigint>();
  for (const { topic, offsets } of endOffsetsByTopic) {
    for (const offset of offsets) endOffsetByTopicPartition.set(`${topic}:${String(offset.partition)}`, offset.high);
  }

  const lag: GroupPartitionLag[] = [];
  for (const entry of committedByTopic) {
    for (const partitionEntry of entry.partitions) {
      const logEndOffset = endOffsetByTopicPartition.get(`${entry.topic}:${String(partitionEntry.partition)}`) ?? 0n;
      const hasCommitted = partitionEntry.offset >= 0n;
      lag.push({
        topic: entry.topic,
        partition: partitionEntry.partition,
        committedOffset: hasCommitted ? partitionEntry.offset.toString() : null,
        logEndOffset: logEndOffset.toString(),
        lag: hasCommitted ? (logEndOffset - partitionEntry.offset).toString() : null,
      });
    }
  }
  return lag;
}

/** Resolves every reset target to a concrete offset before committing — the same client-side resolution `kafka-consumer-groups.sh --reset-offsets` does, since `Admin.setOffsets` only accepts explicit offsets. */
async function resolveResetTargets(
  admin: PooledAdmin,
  topic: string,
  targets: readonly GroupOffsetResetTarget[],
): Promise<{ partition: number; offset: bigint }[]> {
  const needsBounds = targets.some((target) => target.to !== 'offset');
  const bounds = needsBounds ? await admin.fetchTopicOffsets(topic) : [];
  const boundByPartition = new Map(bounds.map((bound) => [bound.partition, bound]));

  const timestampTargets = targets.filter(
    (target): target is Extract<GroupOffsetResetTarget, { to: 'timestamp' }> => target.to === 'timestamp',
  );
  const resolvedByTimestamp = new Map<number, bigint>();
  await Promise.all(
    timestampTargets.map(async (target) => {
      const entries = await admin.fetchTopicOffsetsByTimestamp(topic, target.timestamp);
      const match = entries.find((entry) => entry.partition === target.partition);
      if (match !== undefined && match.offset >= 0n) resolvedByTimestamp.set(target.partition, match.offset);
    }),
  );

  return targets.map((target) => {
    if (target.to === 'offset') return { partition: target.partition, offset: BigInt(target.offset) };

    const bound = boundByPartition.get(target.partition);
    if (target.to === 'earliest') return { partition: target.partition, offset: bound?.low ?? 0n };
    if (target.to === 'latest') return { partition: target.partition, offset: bound?.high ?? 0n };
    // 'timestamp' with no message at or after it falls back to the partition's latest offset.
    return { partition: target.partition, offset: resolvedByTimestamp.get(target.partition) ?? bound?.high ?? 0n };
  });
}

function isGroupErrored(group: { readonly errorCode: number }): boolean {
  return group.errorCode !== 0;
}

/** The two `DeleteGroups` failure codes this route recognizes by name — stable identifiers from the Kafka wire protocol, not values this codebase invents. */
const DELETE_GROUP_ERROR_TYPES: Readonly<Record<number, string>> = {
  68: 'NON_EMPTY_GROUP',
  69: 'GROUP_ID_NOT_FOUND',
};

/** Consumer group inspection, offset reset/delete, member removal, and read-only share group inspection (KIP-932). */
export function registerGroupRoutes(router: Router, context: GroupsRouteContext): void {
  router.get('/api/groups', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { groups } = await admin.listGroups();
    const response: GroupListResponse = {
      groups: groups
        .filter((group) => group.protocolType === CONSUMER_PROTOCOL_TYPE)
        .map((group) => ({ groupId: group.groupId, protocolType: group.protocolType })),
    };
    sendJson(res, 200, response);
  });

  router.get('/api/groups/:id', async (_req, res, params) => {
    const groupId = requireParam(params, 'id');
    const admin = await context.pool.get(context.getActiveProfile());
    const { groups } = await admin.describeGroups([groupId]);
    const group = groups[0];
    // `describeGroups` never throws for an unknown id — a coordinator that has never heard of this
    // group id, or has fully forgotten it, replies with `errorCode: 0` and `state: "Dead"` instead
    // (the same case `kafka group describe` in this repo's CLI already has to check for).
    if (group === undefined || isGroupErrored(group) || group.state === 'Dead') {
      sendError(res, 404, 'unknown_group', `group "${groupId}" not found`);
      return;
    }

    const partitionLag = await computePartitionLag(admin, groupId);
    const response: GroupDetailResponse = {
      groupId: group.groupId,
      state: group.state,
      protocolType: group.protocolType,
      assignorName: group.protocol,
      members: group.members.map(toGroupMember),
      partitionLag,
    };
    sendJson(res, 200, response);
  });

  router.post('/api/groups/:id/offsets/reset', async (req, res, params) => {
    const groupId = requireParam(params, 'id');
    const body = await readJsonBody(req);
    const parsed = resetGroupOffsetsRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid offset reset request', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const resolved = await resolveResetTargets(admin, parsed.data.topic, parsed.data.partitions);
    await admin.setOffsets({ groupId, topic: parsed.data.topic, partitions: resolved });

    const response: ResetGroupOffsetsResponse = {
      groupId,
      topic: parsed.data.topic,
      partitions: resolved.map((entry) => ({ partition: entry.partition, offset: entry.offset.toString() })),
    };
    sendJson(res, 200, response);
  });

  router.delete('/api/groups/:id/offsets', async (req, res, params) => {
    const groupId = requireParam(params, 'id');
    const body = await readJsonBody(req);
    const parsed = deleteGroupOffsetsRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid delete offsets request', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const result = await admin.deleteGroupOffsets({
      groupId,
      topics: parsed.data.topics.map((entry) => ({ topic: entry.topic, partitions: entry.partitions })),
    });

    const response: DeleteGroupOffsetsResponse = {
      groupId,
      topics: result.topics.map((topic) => ({
        topic: topic.name,
        partitions: topic.partitions.map((partition) => ({
          partition: partition.partitionIndex,
          errorCode: partition.errorCode,
        })),
      })),
    };
    sendJson(res, 200, response);
  });

  router.post('/api/groups/:id/members/remove', async (req, res, params) => {
    const groupId = requireParam(params, 'id');
    const body = await readJsonBody(req);
    const parsed = removeGroupMembersRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid remove members request', { issues: parsed.error.issues });
      return;
    }

    const admin = await context.pool.get(context.getActiveProfile());
    const result = await admin.removeMembersFromConsumerGroup({
      groupId,
      members: parsed.data.members.map((member) => ({
        memberId: member.memberId,
        groupInstanceId: member.groupInstanceId,
      })),
    });

    const response: RemoveGroupMembersResponse = { groupId, members: result.members };
    sendJson(res, 200, response);
  });

  router.delete('/api/groups/:id', async (_req, res, params) => {
    const groupId = requireParam(params, 'id');
    const admin = await context.pool.get(context.getActiveProfile());
    const [result] = await admin.deleteGroups([groupId]);
    if (result !== undefined && result.errorCode !== 0) {
      const type = DELETE_GROUP_ERROR_TYPES[result.errorCode];
      throw Object.assign(new Error(`could not delete group "${groupId}"`), type !== undefined ? { type } : {});
    }
    res.writeHead(204);
    res.end();
  });

  router.get('/api/share-groups', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { groups } = await admin.listGroups();
    const response: ShareGroupListResponse = {
      groups: groups
        .filter((group) => group.protocolType === SHARE_PROTOCOL_TYPE)
        .map((group) => ({ groupId: group.groupId, protocolType: group.protocolType })),
    };
    sendJson(res, 200, response);
  });

  router.get('/api/share-groups/:id', async (_req, res, params) => {
    const groupId = requireParam(params, 'id');
    const admin = await context.pool.get(context.getActiveProfile());
    const { groups } = await admin.describeShareGroups([groupId]);
    const group = groups[0];
    if (group === undefined || isGroupErrored(group)) {
      sendError(res, 404, 'unknown_group', `share group "${groupId}" not found`);
      return;
    }

    const topics = new Set<string>();
    for (const member of group.members) {
      for (const assigned of member.assignment.topicPartitions) topics.add(assigned.topicName);
    }

    const { groups: offsetGroups } =
      topics.size === 0
        ? { groups: [] }
        : await admin.listShareGroupOffsets({
            groups: [{ groupId, topics: [...topics].map((topicName) => ({ topicName })) }],
          });

    const response: ShareGroupDetailResponse = {
      groupId: group.groupId,
      state: group.groupState,
      members: group.members.map((member) => ({
        memberId: member.memberId,
        instanceId: null,
        clientId: member.clientId,
        clientHost: member.clientHost,
        assignedTopicPartitions: member.assignment.topicPartitions.map((entry) => ({
          topic: entry.topicName,
          partitions: entry.partitions,
        })),
      })),
      offsets: (offsetGroups[0]?.topics ?? []).map((topic) => ({
        topic: topic.topicName,
        partitions: topic.partitions.map((partition) => ({
          partition: partition.partitionIndex,
          startOffset: partition.startOffset.toString(),
          lag: partition.lag.toString(),
        })),
      })),
    };
    sendJson(res, 200, response);
  });
}
