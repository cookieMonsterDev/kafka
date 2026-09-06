import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startStudio, type StudioHandle } from '../../src/index';
import type { Runtime } from '../../src/runtime';
import { waitFor } from '../helpers/wait-for';

const BROKERS = process.env.KAFKA_BROKERS ?? 'localhost:9092';
const TOPIC = `kafka-studio-it-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

function testRuntime(): Runtime {
  return {
    argv: [],
    // Never a real, walkable directory — `resolveStudioConnectionConfig` discovers `kafka.config.*`
    // by walking up from `cwd`, and this suite's connection must come only from `KAFKA_BROKERS`
    // below, not whatever config file happens to sit above wherever the test runner was invoked.
    cwd: '/nonexistent-studio-e2e-cwd',
    env: { KAFKA_BROKERS: BROKERS },
    platform: 'linux',
    stdout: { write: () => true },
    stderr: { write: () => true },
    now: () => new Date(),
    exit: () => {
      throw new Error('exit() should not be called by startStudio');
    },
    signal: new AbortController().signal,
  };
}

function api(studio: StudioHandle, method: string, path: string, body?: unknown): Promise<Response> {
  return fetch(new URL(path, studio.url), {
    method,
    headers: {
      'x-kafka-studio-token': studio.token,
      ...(body !== undefined ? { 'content-type': 'application/json' } : {}),
    },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
  });
}

/**
 * Runs the studio's own HTTP server (via the library entry point, `startStudio` — the same one
 * `src/index.test.ts` exercises against a fake connection) against a real broker started by
 * `test/helpers/global-setup.ts`. Unlike that unit suite, every request here reaches an actual
 * Kafka cluster: this is the walking-skeleton path across topics, producing, message reads, and
 * consumer groups that a fake admin/producer can't verify.
 */
describe('studio server against a real broker', () => {
  let studio: StudioHandle;

  beforeAll(async () => {
    studio = await startStudio({ host: '127.0.0.1', browser: 'none' }, testRuntime());
  }, 30_000);

  afterAll(async () => {
    await studio.stop();
  });

  it('reports a healthy, writable server', async () => {
    const res = await api(studio, 'GET', '/api/health');
    expect(res.status).toBe(200);
    await expect(res.json()).resolves.toMatchObject({ readOnly: false });
  });

  it('creates the topic, then lists and describes it', async () => {
    const created = await api(studio, 'POST', '/api/topics', { topic: TOPIC, numPartitions: 1, replicationFactor: 1 });
    expect(created.status).toBe(201);

    const listed = await api(studio, 'GET', '/api/topics');
    const { topics } = (await listed.json()) as { topics: { name: string }[] };
    expect(topics.some((topic) => topic.name === TOPIC)).toBe(true);

    const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
    expect(described.status).toBe(200);
    const detail = (await described.json()) as { partitions: { partitionIndex: number }[] };
    expect(detail.partitions).toHaveLength(1);
  });

  it('rejects creating the same topic twice', async () => {
    const result = await api(studio, 'POST', '/api/topics', { topic: TOPIC });
    expect(result.status).toBe(409);
  });

  it('raises the topic to two partitions', async () => {
    const result = await api(studio, 'POST', `/api/topics/${TOPIC}/partitions`, { count: 2 });
    expect(result.status).toBe(200);

    await waitFor(
      async () => {
        const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
        const detail = (await described.json()) as { partitions: unknown[] };
        return detail.partitions.length === 2 ? true : false;
      },
      { message: `${TOPIC} to reach 2 partitions` },
    );
  });

  it('sets and reads back a topic config', async () => {
    const result = await api(studio, 'PATCH', `/api/topics/${TOPIC}/configs`, { set: { 'retention.ms': '3600000' } });
    expect(result.status).toBe(200);

    await waitFor(
      async () => {
        const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
        const detail = (await described.json()) as { configs: { name: string; value: string | null }[] };
        const retention = detail.configs.find((entry) => entry.name === 'retention.ms');
        return retention?.value === '3600000' ? true : false;
      },
      { message: `${TOPIC} to report the updated retention.ms` },
    );
  });

  it('produces a message and reads it back through the bounded page read', async () => {
    const sent = await api(studio, 'POST', '/api/produce', {
      topic: TOPIC,
      messages: [{ key: 'order-1', value: 'hello studio', partition: 0 }],
    });
    expect(sent.status).toBe(200);
    const { results } = (await sent.json()) as { results: { partition: number; offset: string }[] };
    expect(results).toHaveLength(1);
    expect(() => BigInt(results[0]?.offset ?? '')).not.toThrow();

    const page = await api(studio, 'GET', `/api/topics/${TOPIC}/messages?partition=0&from=earliest&limit=10`);
    expect(page.status).toBe(200);
    const { messages } = (await page.json()) as { messages: { key: string | null; value: string | null }[] };
    expect(messages).toContainEqual(
      expect.objectContaining({
        key: Buffer.from('order-1', 'utf8').toString('base64'),
        value: Buffer.from('hello studio', 'utf8').toString('base64'),
      }),
    );
  });

  it('resolves offsets by timestamp without mutating anything', async () => {
    const result = await api(studio, 'POST', `/api/topics/${TOPIC}/offsets/by-time`, { timestamp: 0, partition: 0 });
    expect(result.status).toBe(200);
    const { offsets } = (await result.json()) as { offsets: { partition: number; offset: string | null }[] };
    expect(offsets).toHaveLength(1);
  });

  it('lists consumer groups without error (none joined yet)', async () => {
    const result = await api(studio, 'GET', '/api/groups');
    expect(result.status).toBe(200);
    const { groups } = (await result.json()) as { groups: unknown[] };
    expect(Array.isArray(groups)).toBe(true);
  });

  it('deletes the topic', async () => {
    const result = await api(studio, 'DELETE', `/api/topics/${TOPIC}`);
    expect(result.status).toBe(204);

    await waitFor(
      async () => {
        const listed = await api(studio, 'GET', '/api/topics');
        const { topics } = (await listed.json()) as { topics: { name: string }[] };
        return topics.some((topic) => topic.name === TOPIC) ? false : true;
      },
      { message: `${TOPIC} to disappear from the topic list` },
    );
  });
});
