import { ConfigOperations, ConfigResourceTypes } from '@cookiemonsterdev/kafka-core';
import {
  alterTopicConfigsRequestSchema,
  createPartitionsRequestSchema,
  createTopicRequestSchema,
  type TopicConfigEntry,
  type TopicDetailResponse,
  type TopicListResponse,
  type TopicPartitionDetail,
  type TopicPartitionSummary,
} from '../../shared/contracts/topic';
import { sendError, sendJson } from '../create-server';
import type { AdminPool, PooledAdmin } from '../kafka/admin-pool';
import { hasErrorName } from '../kafka/has-error-name';
import { readJsonBody } from '../json';
import { requireParam, type Router } from '../router';

export interface TopicsRouteContext {
  readonly pool: AdminPool;
  getActiveProfile(): string | null;
}

async function resolveAdmin(context: TopicsRouteContext): Promise<PooledAdmin> {
  return context.pool.get(context.getActiveProfile());
}

interface DescribeConfigsResource {
  readonly configEntries: readonly {
    readonly configName: string;
    readonly configValue: string | null;
    readonly readOnly: boolean;
    readonly isDefault: boolean;
    readonly isSensitive: boolean;
  }[];
}

/**
 * Prefers `describeTopicPartitions` (KIP-966, richer per-partition detail); falls back to
 * `fetchTopicMetadata` only when the broker is too old to support it — same fallback the CLI's
 * own `topic describe` command uses.
 */
async function describeOnePartitions(admin: PooledAdmin, topic: string): Promise<TopicPartitionSummary[]> {
  try {
    const result = await admin.describeTopicPartitions({ topics: [topic] });
    const found = result.topics[0];
    if (found === undefined) return [];
    return found.partitions.map((partition) => ({
      partitionIndex: partition.partitionIndex,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
    }));
  } catch (error) {
    if (!hasErrorName(error, 'KafkaServerDoesNotSupportApiKey')) throw error;
    const result = await admin.fetchTopicMetadata({ topics: [topic] });
    const found = result.topics[0];
    if (found === undefined) return [];
    return found.partitions.map((partition) => ({
      partitionIndex: partition.partitionId,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
    }));
  }
}

function toConfigEntries(resource: DescribeConfigsResource | undefined): TopicConfigEntry[] {
  if (resource === undefined) return [];
  return resource.configEntries.map((entry) => ({
    name: entry.configName,
    value: entry.configValue,
    isDefault: entry.isDefault,
    readOnly: entry.readOnly,
    isSensitive: entry.isSensitive,
  }));
}

/**
 * Log dirs are reported per replica (one entry per broker holding the partition), so a topic with
 * a replication factor above 1 gets more than one size for the same partition — replicas drift
 * apart slightly under normal replication lag. The largest reported value is the closest read on
 * "how much data this partition holds" without querying the leader specifically.
 */
async function sizeByPartition(
  admin: PooledAdmin,
  topic: string,
  partitionIndexes: readonly number[],
): Promise<Map<number, bigint>> {
  const sizes = new Map<number, bigint>();
  if (partitionIndexes.length === 0) return sizes;

  const { brokers } = await admin.describeLogDirs({ topics: [{ topic, partitions: [...partitionIndexes] }] });
  for (const broker of brokers) {
    for (const logDir of broker.logDirs) {
      if (logDir.errorCode !== 0) continue;
      for (const topicDir of logDir.topics) {
        if (topicDir.topic !== topic) continue;
        for (const partition of topicDir.partitions) {
          const current = sizes.get(partition.partition);
          if (current === undefined || partition.size > current) sizes.set(partition.partition, partition.size);
        }
      }
    }
  }
  return sizes;
}

export function registerTopicRoutes(router: Router, context: TopicsRouteContext): void {
  router.get('/api/topics', async (_req, res) => {
    const admin = await resolveAdmin(context);
    const names = await admin.listTopics();
    const { topics } = await admin.fetchTopicMetadata({ topics: names });
    const response: TopicListResponse = {
      topics: topics.map((topic) => ({
        name: topic.name,
        partitionCount: topic.partitions.length,
        replicationFactor: topic.partitions[0]?.replicas.length ?? null,
      })),
    };
    sendJson(res, 200, response);
  });

  router.get('/api/topics/:name', async (_req, res, params) => {
    const name = requireParam(params, 'name');
    const admin = await resolveAdmin(context);

    // Issued first and awaited alone: on an unknown topic the broker reports
    // `UNKNOWN_TOPIC_OR_PARTITION` for this resource and `describeConfigs` throws — the single
    // source of truth this route uses to answer "does this topic exist?" with a real 404 instead
    // of guessing from an empty partitions array.
    const configsResult = await admin.describeConfigs({
      resources: [{ type: ConfigResourceTypes.TOPIC, name }],
    });

    const partitions = await describeOnePartitions(admin, name);
    const [offsets, sizes] = await Promise.all([
      admin.fetchTopicOffsets(name),
      sizeByPartition(
        admin,
        name,
        partitions.map((partition) => partition.partitionIndex),
      ),
    ]);
    const offsetByPartition = new Map(offsets.map((offset) => [offset.partition, offset]));

    const detailPartitions: TopicPartitionDetail[] = partitions.map((partition) => {
      const offset = offsetByPartition.get(partition.partitionIndex);
      const size = sizes.get(partition.partitionIndex);
      return {
        ...partition,
        earliestOffset: offset ? offset.low.toString() : null,
        latestOffset: offset ? offset.high.toString() : null,
        sizeBytes: size !== undefined ? size.toString() : null,
      };
    });

    const response: TopicDetailResponse = {
      name,
      partitions: detailPartitions,
      configs: toConfigEntries(configsResult.resources[0]),
    };
    sendJson(res, 200, response);
  });

  router.post('/api/topics', async (req, res) => {
    const body = await readJsonBody(req);
    const parsed = createTopicRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid create-topic request', { issues: parsed.error.issues });
      return;
    }

    const { topic, numPartitions, replicationFactor, configEntries } = parsed.data;
    const admin = await resolveAdmin(context);
    const created = await admin.createTopics({
      topics: [
        {
          topic,
          ...(numPartitions !== undefined ? { numPartitions } : {}),
          ...(replicationFactor !== undefined ? { replicationFactor } : {}),
          ...(configEntries !== undefined
            ? { configEntries: Object.entries(configEntries).map(([entryName, value]) => ({ name: entryName, value })) }
            : {}),
        },
      ],
    });

    if (!created) {
      sendError(res, 409, 'topic_already_exists', `topic "${topic}" already exists`);
      return;
    }

    sendJson(res, 201, { topic });
  });

  router.delete('/api/topics/:name', async (_req, res, params) => {
    const name = requireParam(params, 'name');
    const admin = await resolveAdmin(context);
    await admin.deleteTopics({ topics: [name] });
    res.writeHead(204);
    res.end();
  });

  router.post('/api/topics/:name/partitions', async (req, res, params) => {
    const name = requireParam(params, 'name');
    const body = await readJsonBody(req);
    const parsed = createPartitionsRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid add-partitions request', { issues: parsed.error.issues });
      return;
    }

    const admin = await resolveAdmin(context);
    await admin.createPartitions({ topicPartitions: [{ topic: name, count: parsed.data.count }] });
    sendJson(res, 200, { topic: name, count: parsed.data.count });
  });

  router.patch('/api/topics/:name/configs', async (req, res, params) => {
    const name = requireParam(params, 'name');
    const body = await readJsonBody(req);
    const parsed = alterTopicConfigsRequestSchema.safeParse(body);
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'invalid config-update request', { issues: parsed.error.issues });
      return;
    }

    const set = Object.entries(parsed.data.set ?? {}).map(([configName, value]) => ({
      name: configName,
      value,
      operation: ConfigOperations.SET,
    }));
    const unset = (parsed.data.unset ?? []).map((configName) => ({
      name: configName,
      value: null,
      operation: ConfigOperations.DELETE,
    }));

    const admin = await resolveAdmin(context);
    await admin.incrementalAlterConfigs({
      resources: [{ type: ConfigResourceTypes.TOPIC, name, configs: [...set, ...unset] }],
    });
    sendJson(res, 200, { topic: name });
  });
}
