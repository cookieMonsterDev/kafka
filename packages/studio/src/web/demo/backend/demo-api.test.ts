import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ScanResult } from '../../../shared/contracts/debug';
import type { StudioEvent } from '../../../shared/contracts/event';
import type { GroupDetailResponse, RemoveGroupMembersResponse } from '../../../shared/contracts/group';
import type { MessageRecord, MessagesPageResponse } from '../../../shared/contracts/message';
import type { BurstProgress, ProduceResponse } from '../../../shared/contracts/produce';
import type { TopicDetailResponse, TopicListResponse } from '../../../shared/contracts/topic';
import { DemoApi } from './demo-api';

const BASE = 'http://studio.test';

function call(api: DemoApi, method: string, path: string, body?: unknown): Response {
  return api.handle(method, new URL(path, BASE), body);
}

async function callJson<T>(api: DemoApi, method: string, path: string, body?: unknown): Promise<T> {
  const response = call(api, method, path, body);
  expect(response.ok, `${method} ${path} → ${String(response.status)}`).toBe(true);
  return (await response.json()) as T;
}

function decode(value: string | null): string | null {
  return value === null ? null : new TextDecoder().decode(Uint8Array.from(atob(value), (char) => char.charCodeAt(0)));
}

interface Frame {
  readonly event: string;
  readonly data: unknown;
}

function openStream(api: DemoApi, path: string): { frames: Frame[]; ended: () => boolean; close: () => void } {
  const frames: Frame[] = [];
  let ended = false;
  const cleanup = api.openStream(
    new URL(path, BASE),
    (event, data) => frames.push({ event, data }),
    () => {
      ended = true;
    },
  );
  if (cleanup === null) throw new Error(`no stream at ${path}`);
  return { frames, ended: () => ended, close: cleanup };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('DemoApi', () => {
  it('reports a connected three-broker cluster with seeded topics', async () => {
    const api = new DemoApi();

    await expect(callJson(api, 'GET', '/api/cluster')).resolves.toMatchObject({ connected: true, brokerCount: 3 });
    const { topics } = await callJson<TopicListResponse>(api, 'GET', '/api/topics');
    expect(topics.map((topic) => topic.name)).toEqual([
      'audit-log',
      'customer-profiles',
      'inventory.updates',
      'orders',
      'payments',
    ]);
  });

  it('is deterministic for the same seed, so every visit opens on the same data', async () => {
    const page = (api: DemoApi) => callJson<MessagesPageResponse>(api, 'GET', '/api/topics/orders/messages?limit=5');
    const [first, second] = await Promise.all([page(new DemoApi()), page(new DemoApi())]);
    expect(first.messages.map((message) => message.value)).toEqual(second.messages.map((message) => message.value));
  });

  it('returns a produced record from the next tail read, base64-encoded like the real server', async () => {
    const api = new DemoApi();

    const produced = await callJson<ProduceResponse>(api, 'POST', '/api/produce', {
      topic: 'audit-log',
      messages: [{ key: 'k-1', value: 'hello', headers: { origin: 'test' } }],
    });
    const page = await callJson<MessagesPageResponse>(api, 'GET', '/api/topics/audit-log/messages?limit=1');

    const message = page.messages[0];
    expect(message?.offset).toBe(produced.results[0]?.offset);
    expect(decode(message?.key ?? null)).toBe('k-1');
    expect(decode(message?.value ?? null)).toBe('hello');
    expect(decode(message?.headers.origin ?? null)).toBe('test');
  });

  it('uses the server error envelope for invalid and conflicting requests', async () => {
    const api = new DemoApi();

    const duplicate = call(api, 'POST', '/api/topics', { topic: 'orders' });
    expect(duplicate.status).toBe(409);
    await expect(duplicate.json()).resolves.toEqual({
      error: { code: 'topic_already_exists', message: 'topic "orders" already exists' },
    });
    expect(call(api, 'POST', '/api/topics', { topic: 'bad name!' }).status).toBe(400);
    expect(call(api, 'GET', '/api/topics/missing').status).toBe(404);
  });

  it('creates a topic with config overrides and reports them as non-default', async () => {
    const api = new DemoApi();

    await callJson(api, 'POST', '/api/topics', {
      topic: 'shipments',
      numPartitions: 2,
      replicationFactor: 3,
      configEntries: { 'retention.ms': '3600000' },
    });
    const detail = await callJson<TopicDetailResponse>(api, 'GET', '/api/topics/shipments');

    expect(detail.partitions.map((partition) => partition.replicas.length)).toEqual([3, 3]);
    expect(detail.configs.find((config) => config.name === 'retention.ms')).toMatchObject({
      value: '3600000',
      isDefault: false,
    });
  });

  it('rejects an offset reset while the group has members, as a broker does', async () => {
    const api = new DemoApi();
    const reset = { topic: 'orders', partitions: [{ partition: 0, to: 'latest' }] };

    expect(call(api, 'POST', '/api/groups/order-fulfillment/offsets/reset', reset).status).toBe(409);

    const group = await callJson<GroupDetailResponse>(api, 'GET', '/api/groups/order-fulfillment');
    const removed = await callJson<RemoveGroupMembersResponse>(
      api,
      'POST',
      '/api/groups/order-fulfillment/members/remove',
      { members: [...group.members.map((member) => ({ memberId: member.memberId })), { memberId: 'gone' }] },
    );
    // 25 is UNKNOWN_MEMBER_ID, what a broker reports for a member id the group doesn't know.
    expect(removed.members.map((member) => member.errorCode)).toEqual([0, 0, 0, 25]);
    await callJson(api, 'POST', '/api/groups/order-fulfillment/offsets/reset', reset);

    const after = await callJson<GroupDetailResponse>(api, 'GET', '/api/groups/order-fulfillment');
    expect(after.state).toBe('Empty');
    expect(after.partitionLag.find((entry) => entry.partition === 0)?.lag).toBe('0');
  });

  it('streams tailed records and reports the tail as consume activity', async () => {
    const api = new DemoApi();
    const tail = openStream(api, '/api/topics/payments/tail?partition=1');
    const activity = openStream(api, '/api/events');

    await callJson(api, 'POST', '/api/produce', { topic: 'payments', messages: [{ value: 'p1', partition: 1 }] });
    await callJson(api, 'POST', '/api/produce', { topic: 'payments', messages: [{ value: 'p0', partition: 0 }] });

    expect(tail.frames.map((frame) => decode((frame.data as MessageRecord).value))).toEqual(['p1']);
    expect(activity.frames.map((frame) => (frame.data as StudioEvent).kind)).toEqual(['produce', 'consume', 'produce']);
  });

  it('ends a tail of an unknown topic with a tail-error frame', () => {
    const api = new DemoApi();
    const tail = openStream(api, '/api/topics/missing/tail');

    expect(tail.frames).toEqual([{ event: 'tail-error', data: { message: 'topic "missing" does not exist' } }]);
    expect(tail.ended()).toBe(true);
  });

  it('streams debug scan progress and a result over a fetch response body', async () => {
    const api = new DemoApi();
    await callJson(api, 'POST', '/api/produce', {
      topic: 'orders',
      messages: [{ key: 'needle', value: JSON.stringify({ orderId: 'needle', total: 1 }) }],
    });

    const response = call(api, 'POST', '/api/debug/scan', {
      topics: ['orders'],
      predicates: [{ field: 'value.orderId', operator: 'eq', value: 'needle' }],
      maxRecords: 10_000,
    });
    const text = await response.text();

    expect(response.headers.get('content-type')).toBe('text/event-stream');
    const result = JSON.parse(/event: result\ndata: (.*)\n\n/.exec(text)?.[1] ?? 'null') as ScanResult;
    expect(result.matched).toBe(1);
    expect(result.records[0]?.topic).toBe('orders');
    expect(result.complete).toBe(true);
  });

  it('runs a burst at its rate and reports progress until it completes', async () => {
    vi.useFakeTimers();
    const api = new DemoApi();

    const { jobId } = await callJson<{ jobId: string }>(api, 'POST', '/api/produce/burst', {
      topic: 'audit-log',
      template: { key: 'k-{{seq}}', value: 'v-{{seq}}' },
      count: 25,
      ratePerSecond: 100,
    });
    const progress = openStream(api, `/api/produce/burst/${jobId}`);
    vi.advanceTimersByTime(300);

    const last = progress.frames.at(-1)?.data as BurstProgress;
    expect(last).toMatchObject({ sent: 25, total: 25, status: 'completed' });
    expect(progress.ended()).toBe(true);
    const page = await callJson<MessagesPageResponse>(api, 'GET', '/api/topics/audit-log/messages?limit=1');
    expect(decode(page.messages[0]?.key ?? null)).toBe('k-24');
  });

  it('answers demo shell commands and explains the ones that need an installed Studio', async () => {
    const api = new DemoApi();
    const { id } = await callJson<{ id: string }>(api, 'POST', '/api/terminal/sessions');
    const shell = openStream(api, `/api/terminal/sessions/${id}/events`);
    const output = () => shell.frames.map((frame) => (frame.data as { data?: string }).data ?? '').join('');

    await callJson(api, 'POST', `/api/terminal/sessions/${id}/input`, { data: 'kafka topic list\r' });
    expect(output()).toContain('orders\r\npayments');

    await callJson(api, 'POST', `/api/terminal/sessions/${id}/input`, { data: 'topic delete orders\r' });
    expect(output()).toContain(`isn't available in the browser demo`);

    await callJson(api, 'POST', `/api/terminal/sessions/${id}/input`, { data: 'exit\r' });
    expect(shell.frames.at(-1)?.event).toBe('exit');
    expect(shell.ended()).toBe(true);
  });

  it('reports input to a closed shell session as a 404, like the real server', () => {
    const response = call(new DemoApi(), 'POST', '/api/terminal/sessions/missing/input', { data: 'x' });

    expect(response.status).toBe(404);
  });

  it('refuses a tail with an invalid partition instead of streaming nothing', () => {
    expect(
      new DemoApi().openStream(
        new URL('/api/topics/orders/tail?partition=abc', BASE),
        () => {},
        () => {},
      ),
    ).toBeNull();
  });

  it('explains that features without a demo simulation need an installed Studio', async () => {
    const response = call(new DemoApi(), 'GET', '/api/debug/schemas');

    expect(response.status).toBe(501);
    await expect(response.json()).resolves.toMatchObject({ error: { code: 'not_in_demo' } });
  });
});
