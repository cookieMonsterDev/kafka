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

/** Status plus body (read from a clone), so a failed status assertion also shows the error the server sent. */
async function statusOf(response: Response): Promise<{ status: number; body: string }> {
  return { status: response.status, body: await response.clone().text() };
}

async function openShell(studio: StudioHandle) {
  const response = await api(studio, 'POST', '/api/terminal/sessions');
  expect(response.status).toBe(201);
  const { id } = (await response.json()) as { id: string };
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20_000);
  const events = await fetch(`${studio.url}api/terminal/sessions/${id}/events`, {
    headers: { 'x-kafka-studio-token': studio.token },
    signal: controller.signal,
  });
  const reader = events.body?.getReader() as ReadableStreamDefaultReader<Uint8Array> | undefined;
  if (reader === undefined) throw new Error('No shell output stream');
  const terminalReader = reader;
  let output = '';
  let pending = '';
  const decoder = new TextDecoder();
  async function until(fragment: string, start = 0): Promise<void> {
    while (!output.slice(start).includes(fragment)) {
      const chunk = await terminalReader.read();
      if (chunk.done) throw new Error(`Shell closed before ${fragment}`);
      pending += decoder.decode(chunk.value, { stream: true });
      let boundary = pending.indexOf('\n\n');
      while (boundary >= 0) {
        const frame = pending.slice(0, boundary);
        pending = pending.slice(boundary + 2);
        const data = frame.split('\n').find((line) => line.startsWith('data: '));
        if (data !== undefined) output += (JSON.parse(data.slice(6)) as { data?: string }).data ?? '';
        boundary = pending.indexOf('\n\n');
      }
    }
  }
  await until(':~$ ');
  return {
    async run(line: string, marker: string) {
      const start = output.length;
      const sent = await api(studio, 'POST', `/api/terminal/sessions/${id}/input`, { data: `${line}\r` });
      expect(sent.status).toBe(200);
      await until(marker, start);
      await until(':~$ ', output.indexOf(marker, start));
      return output.slice(start);
    },
    async close() {
      clearTimeout(timer);
      controller.abort();
      await api(studio, 'DELETE', `/api/terminal/sessions/${id}`);
    },
  };
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
    expect(await statusOf(res)).toEqual({ status: 200, body: expect.any(String) });
    await expect(res.json()).resolves.toMatchObject({ readOnly: false });
  });

  it('creates the topic, then lists and describes it', async () => {
    const created = await api(studio, 'POST', '/api/topics', { topic: TOPIC, numPartitions: 1, replicationFactor: 1 });
    expect(await statusOf(created)).toEqual({ status: 201, body: expect.any(String) });

    const listed = await api(studio, 'GET', '/api/topics');
    const { topics } = (await listed.json()) as { topics: { name: string }[] };
    expect(topics.some((topic) => topic.name === TOPIC)).toBe(true);

    const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
    expect(await statusOf(described)).toEqual({ status: 200, body: expect.any(String) });
    const detail = (await described.json()) as { partitions: { partitionIndex: number }[] };
    expect(detail.partitions).toHaveLength(1);
  });

  it('rejects creating the same topic twice', async () => {
    const result = await api(studio, 'POST', '/api/topics', { topic: TOPIC });
    expect(await statusOf(result)).toEqual({ status: 409, body: expect.any(String) });
  });

  it('raises the topic to two partitions', async () => {
    const result = await api(studio, 'POST', `/api/topics/${TOPIC}/partitions`, { count: 2 });
    expect(await statusOf(result)).toEqual({ status: 200, body: expect.any(String) });

    await waitFor(
      async () => {
        const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
        if (!described.ok) return false;
        const detail = (await described.json()) as { partitions: unknown[] };
        return detail.partitions.length === 2 ? true : false;
      },
      { message: `${TOPIC} to reach 2 partitions` },
    );
  });

  it('sets and reads back a topic config', async () => {
    const result = await api(studio, 'PATCH', `/api/topics/${TOPIC}/configs`, { set: { 'retention.ms': '3600000' } });
    expect(await statusOf(result)).toEqual({ status: 200, body: expect.any(String) });

    await waitFor(
      async () => {
        const described = await api(studio, 'GET', `/api/topics/${TOPIC}`);
        if (!described.ok) return false;
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
    expect(await statusOf(sent)).toEqual({ status: 200, body: expect.any(String) });
    const { results } = (await sent.json()) as { results: { partition: number; offset: string }[] };
    expect(results).toHaveLength(1);
    expect(() => BigInt(results[0]?.offset ?? '')).not.toThrow();

    const page = await api(studio, 'GET', `/api/topics/${TOPIC}/messages?partition=0&from=earliest&limit=10`);
    expect(await statusOf(page)).toEqual({ status: 200, body: expect.any(String) });
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
    expect(await statusOf(result)).toEqual({ status: 200, body: expect.any(String) });
    const { offsets } = (await result.json()) as { offsets: { partition: number; offset: string | null }[] };
    expect(offsets).toHaveLength(1);
  });

  it('lists consumer groups without error (none joined yet)', async () => {
    const result = await api(studio, 'GET', '/api/groups');
    expect(await statusOf(result)).toEqual({ status: 200, body: expect.any(String) });
    const { groups } = (await result.json()) as { groups: unknown[] };
    expect(Array.isArray(groups)).toBe(true);
  });

  it('runs the embedded CLI and Studio message helpers through the terminal', async () => {
    const shell = await openShell(studio);
    const topic = `${TOPIC}-shell`;
    try {
      const created = await shell.run(
        `topic create ${topic} --partitions 1 --replication-factor 1 --json`,
        '"ok":true',
      );
      expect(created).toContain(topic);
      const produced = await shell.run(`produce ${topic} --value terminal-message --json`, '"results":');
      expect(produced).toContain('"offset":');
      const consumed = await shell.run(`consume ${topic} --from earliest --limit 10 --json`, '"messages":');
      expect(consumed).toContain(Buffer.from('terminal-message').toString('base64'));
      const listed = await shell.run('topic list --json', '"topics":');
      expect(listed).toContain(topic);
    } finally {
      await shell.close();
      await api(studio, 'DELETE', `/api/topics/${topic}`);
    }
  });

  it('enforces read-only mode on terminal mutations against a real broker', async () => {
    const readonly = await startStudio({ browser: 'none', readOnly: true }, testRuntime());
    const shell = await openShell(readonly);
    const topic = `${TOPIC}-forbidden`;
    try {
      const output = await shell.run(`topic create ${topic} --partitions 1`, 'read-only mode');
      expect(output).toContain('exit 5');
      const topics = await api(readonly, 'GET', '/api/topics');
      expect(await topics.text()).not.toContain(topic);
    } finally {
      await shell.close();
      await readonly.stop();
    }
  });

  it('deletes the topic', async () => {
    const result = await api(studio, 'DELETE', `/api/topics/${TOPIC}`);
    expect(await statusOf(result)).toEqual({ status: 204, body: expect.any(String) });

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
