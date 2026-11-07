import { ConfigResourceTypes, Partitioners } from '@cookiemonsterdev/kafka-core';
import { z } from 'zod';
import { setTimeout as delay } from 'node:timers/promises';
import {
  replayRequestSchema,
  scanRequestSchema,
  scenarioSchema,
  type ConfigDifference,
  type ReplayResult,
} from '../../shared/contracts/debug';
import { topicNameSchema } from '../../shared/contracts/topic';
import { sendError, sendJson } from '../create-server';
import { readJsonBody } from '../json';
import type { AdminPool, PooledAdmin } from '../kafka/admin-pool';
import { isKnownProfile, type StudioConnectionConfig } from '../kafka/connection';
import type { MessageConsumerFactory } from '../kafka/messages';
import type { ProducerPool } from '../kafka/produce';
import { scanMessages } from '../kafka/scan';
import { runScenario } from '../kafka/scenario';
import type { StudioSchemaRegistry, RegistrySchema } from '../kafka/schema-registry';
import type { Router } from '../router';
import { openSseStream } from '../sse';

export interface DebugRouteContext {
  readonly pool: AdminPool;
  readonly producers: ProducerPool;
  readonly consumerFactory: MessageConsumerFactory;
  readonly connection: StudioConnectionConfig;
  readonly registry: StudioSchemaRegistry;
  getActiveProfile(): string | null;
}

function decodeBytes(value: string | null): Buffer | null {
  return value === null ? null : Buffer.from(value, 'base64');
}

async function topicSnapshot(admin: PooledAdmin, topic: string): Promise<Map<string, string | null>> {
  const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
  const found = metadata.topics.find((entry) => entry.name === topic);
  if (!found) throw new Error(`Topic ${topic} is unavailable`);
  const configs = await admin.describeConfigs({ resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }] });
  const resource = configs.resources[0];
  if (!resource || resource.errorCode !== 0) throw new Error(`Could not read configurations for ${topic}`);
  const values = new Map<string, string | null>([['partitions', String(found.partitions.length)]]);
  for (const entry of resource.configEntries) if (!entry.isSensitive) values.set(entry.configName, entry.configValue);
  for (const partition of found.partitions)
    values.set(`replicas.partition-${String(partition.partitionId)}`, String(partition.replicas.length));
  return values;
}

/** All long operations capture their profile once and stop when the response connection closes. */
export function registerDebugRoutes(router: Router, context: DebugRouteContext): void {
  let activeOperations = 0;
  const reserve = (): (() => void) => {
    if (activeOperations >= 4) throw new Error('Four debugger operations are already running; cancel one first');
    activeOperations++;
    return () => {
      activeOperations--;
    };
  };
  router.post(
    '/api/debug/scan',
    async (req, res) => {
      const parsed = scanRequestSchema.safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'Invalid scan request', parsed.error.issues);
        return;
      }
      const release = reserve();
      const profile = context.getActiveProfile();
      const controller = new AbortController();
      const stream = openSseStream(req, res, () => controller.abort(), true);
      try {
        const admin = await context.pool.get(profile);
        const result = await scanMessages(
          admin,
          context.consumerFactory,
          profile,
          parsed.data,
          controller.signal,
          (progress) =>
            stream.send('progress', {
              scanned: progress.scanned,
              matched: progress.matched,
              coverage: progress.coverage,
            }),
        );
        stream.send('result', result);
      } catch (error) {
        stream.send('operation-error', { message: error instanceof Error ? error.message : 'Scan failed' });
      } finally {
        release();
        stream.close();
      }
    },
    { allowInReadOnly: true },
  );

  router.post('/api/debug/replay', async (req, res) => {
    const parsed = replayRequestSchema.safeParse(await readJsonBody(req));
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'Invalid replay request', parsed.error.issues);
      return;
    }
    const release = reserve();
    const profile = context.getActiveProfile();
    const controller = new AbortController();
    const stream = openSseStream(req, res, () => controller.abort(), true);
    try {
      const producer = await context.producers.get(profile);
      for (const [index, record] of parsed.data.records.entries()) {
        controller.signal.throwIfAborted();
        let result: ReplayResult;
        try {
          await producer.send({
            topic: parsed.data.topic,
            messages: [
              {
                key: decodeBytes(record.key),
                value: decodeBytes(record.value),
                headers: Object.fromEntries(
                  Object.entries(record.headers).map(([key, value]) => [
                    key,
                    Array.isArray(value) ? value.map(decodeBytes) : decodeBytes(value),
                  ]),
                ),
              },
            ],
            signal: controller.signal,
          });
          result = { index, status: 'sent' };
        } catch (error) {
          result = {
            index,
            status: 'failed',
            error: error instanceof Error ? error.message : 'Send failed; delivery may be uncertain',
          };
        }
        stream.send('record', result);
        if (index + 1 < parsed.data.records.length)
          await delay(1000 / parsed.data.ratePerSecond, undefined, { signal: controller.signal });
      }
      stream.send('done', {});
    } catch (error) {
      stream.send('operation-error', { message: error instanceof Error ? error.message : 'Replay stopped' });
    } finally {
      release();
      stream.close();
    }
  });

  router.post('/api/debug/scenario', async (req, res) => {
    const parsed = scenarioSchema.safeParse(await readJsonBody(req));
    if (!parsed.success) {
      sendError(res, 400, 'bad_request', 'Invalid scenario', parsed.error.issues);
      return;
    }
    const release = reserve();
    const profile = context.getActiveProfile();
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(120000)]);
    const stream = openSseStream(req, res, () => controller.abort(), true);
    try {
      const [admin, producer] = await Promise.all([context.pool.get(profile), context.producers.get(profile)]);
      const result = await runScenario(parsed.data, admin, producer, context.consumerFactory, profile, signal, (step) =>
        stream.send('step', step),
      );
      stream.send('result', result);
    } catch (error) {
      stream.send('operation-error', { message: error instanceof Error ? error.message : 'Scenario stopped' });
    } finally {
      release();
      stream.close();
    }
  });

  const compareSchema = z.object({ topic: topicNameSchema, left: z.string().nullable(), right: z.string().nullable() });
  router.post(
    '/api/debug/compare',
    async (req, res) => {
      const parsed = compareSchema.safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'Invalid comparison');
        return;
      }
      for (const profile of [parsed.data.left, parsed.data.right]) {
        if (profile !== null && !isKnownProfile(context.connection, profile)) {
          sendError(res, 404, 'unknown_profile', 'Unknown comparison profile');
          return;
        }
      }
      const [leftAdmin, rightAdmin] = await Promise.all([
        context.pool.get(parsed.data.left),
        context.pool.get(parsed.data.right),
      ]);
      const [left, right] = await Promise.all([
        topicSnapshot(leftAdmin, parsed.data.topic),
        topicSnapshot(rightAdmin, parsed.data.topic),
      ]);
      const differences: ConfigDifference[] = [];
      for (const field of new Set([...left.keys(), ...right.keys()])) {
        const a = left.get(field) ?? null;
        const b = right.get(field) ?? null;
        if (a !== b) differences.push({ field, left: a, right: b });
      }
      const subject = `${parsed.data.topic}-value`;
      async function schemaSnapshot(profile: string | null) {
        try {
          return { status: 'available', ...(await context.registry.snapshot(profile, subject)) };
        } catch (error) {
          return { status: 'unavailable', message: error instanceof Error ? error.message : 'Registry unavailable' };
        }
      }
      const [leftSchema, rightSchema] = await Promise.all([
        schemaSnapshot(parsed.data.left),
        schemaSnapshot(parsed.data.right),
      ]);
      sendJson(res, 200, { differences, schemas: { left: leftSchema, right: rightSchema } });
    },
    { allowInReadOnly: true },
  );

  router.post(
    '/api/debug/partition',
    async (req, res) => {
      const parsed = z
        .object({ topic: topicNameSchema, key: z.string().max(100000), partitioner: z.enum(['default', 'legacy']) })
        .safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'Invalid partition inspection');
        return;
      }
      const admin = await context.pool.get(context.getActiveProfile());
      const metadata = await admin.fetchTopicMetadata({ topics: [parsed.data.topic] });
      const topic = metadata.topics.find((entry) => entry.name === parsed.data.topic);
      if (!topic || topic.partitions.length === 0) {
        sendError(res, 404, 'unknown_topic', 'Topic unavailable');
        return;
      }
      const factory =
        parsed.data.partitioner === 'default' ? Partitioners.DefaultPartitioner : Partitioners.LegacyPartitioner;
      const partition = factory()({
        topic: parsed.data.topic,
        partitionMetadata: topic.partitions,
        message: { key: parsed.data.key, value: null },
      });
      sendJson(res, 200, { partition, partitionCount: topic.partitions.length });
    },
    { allowInReadOnly: true },
  );

  router.get('/api/debug/schemas', async (_req, res, _params, url) => {
    const profile = context.getActiveProfile();
    const subject = url.searchParams.get('subject');
    if (subject === null) {
      sendJson(res, 200, { subjects: await context.registry.get<string[]>(profile, 'subjects') });
      return;
    }
    const version = url.searchParams.get('version');
    const path = `subjects/${encodeURIComponent(subject)}/versions`;
    if (version === null) {
      sendJson(res, 200, { versions: await context.registry.get<number[]>(profile, path) });
      return;
    }
    if (version !== 'latest' && !/^\d+$/.test(version)) {
      sendError(res, 400, 'bad_request', 'Invalid schema version');
      return;
    }
    sendJson(res, 200, await context.registry.get<RegistrySchema>(profile, `${path}/${version}`));
  });

  router.post(
    '/api/debug/schemas/decode',
    async (req, res) => {
      const parsed = z.object({ value: z.string().max(1000000) }).safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'Invalid schema payload');
        return;
      }
      sendJson(res, 200, { value: await context.registry.decode(context.getActiveProfile(), parsed.data.value) });
    },
    { allowInReadOnly: true },
  );

  router.post(
    '/api/debug/schemas/encode',
    async (req, res) => {
      const parsed = z
        .object({
          id: z.number().int().positive().max(2147483647),
          value: z.unknown(),
          messageType: z.string().max(256).optional(),
        })
        .safeParse(await readJsonBody(req));
      if (!parsed.success) {
        sendError(res, 400, 'bad_request', 'Invalid schema payload');
        return;
      }
      const encoded = await context.registry.encode(
        context.getActiveProfile(),
        parsed.data.id,
        parsed.data.value,
        parsed.data.messageType,
      );
      sendJson(res, 200, { value: encoded.toString('base64') });
    },
    { allowInReadOnly: true },
  );
}
