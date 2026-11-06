import { afterEach, describe, expect, it, vi } from 'vitest';
import { runConsoleCommand } from './console-api';

vi.mock('./auth', () => ({
  rejectSession: vi.fn(),
  withAuthHeaders: () => new Headers({ 'x-kafka-studio-token': 'test-session' }),
}));
afterEach(() => vi.unstubAllGlobals());

function mockResponse(body: unknown, status = 200) {
  const fetch = vi.fn().mockResolvedValue(Response.json(body, { status }));
  vi.stubGlobal('fetch', fetch);
  return fetch;
}

describe('console read dispatcher', () => {
  it.each([
    ['topic list', '/api/topics'],
    ['topic describe orders', '/api/topics/orders'],
    ['config describe --type topic orders', '/api/topics/orders'],
    ['group list', '/api/groups'],
    ['group describe "payments worker"', '/api/groups/payments%20worker'],
    ['acl list', '/api/acls'],
    ['quota describe', '/api/quotas'],
    ['txn list', '/api/transactions'],
    ['cluster info', '/api/cluster'],
  ])('uses an authenticated GET for %s', async (line, path) => {
    const fetch = mockResponse({ connected: true, configs: [] });
    await runConsoleCommand(line);
    expect(fetch).toHaveBeenCalledOnce();
    const [url, options] = fetch.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(path);
    expect(options.method).toBeUndefined();
    expect(new Headers(options.headers).get('x-kafka-studio-token')).toBe('test-session');
  });

  it('preserves decimal offsets without converting to lossy numbers', async () => {
    mockResponse({
      name: 'orders',
      partitions: [{ partitionIndex: 0, earliestOffset: '9007199254740993', latestOffset: '9007199254740999' }],
    });
    await expect(runConsoleCommand('topic offsets orders')).resolves.toEqual({
      topic: 'orders',
      partitions: [{ partition: 0, earliestOffset: '9007199254740993', latestOffset: '9007199254740999' }],
    });
  });

  it('reports committed group offsets and lag', async () => {
    const partitionLag = [{ topic: 'orders', partition: 0, committedOffset: null, logEndOffset: '42', lag: null }];
    mockResponse({ groupId: 'reader', partitionLag });
    await expect(runConsoleCommand('group offsets reader')).resolves.toEqual({
      groupId: 'reader',
      partitions: partitionLag,
    });
  });

  it('fails visibly when the cluster is disconnected or the API rejects a read', async () => {
    mockResponse({ connected: false });
    await expect(runConsoleCommand('cluster info')).rejects.toThrow('Not connected');
    mockResponse({ error: { message: 'Kafka authorization failed' } }, 403);
    await expect(runConsoleCommand('topic list')).rejects.toThrow('Kafka authorization failed');
  });

  it('rejects writes, connection overrides and shell input before any request', async () => {
    const fetch = mockResponse({});
    for (const line of [
      'topic delete orders',
      'topic list --profile prod',
      'topic list && topic delete orders',
      'admin call deleteTopics',
    ]) {
      await expect(runConsoleCommand(line)).rejects.toThrow();
    }
    expect(fetch).not.toHaveBeenCalled();
  });

  it('provides help locally', async () => {
    const fetch = mockResponse({});
    await expect(runConsoleCommand('help')).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ command: 'topic list' })]),
    );
    await expect(runConsoleCommand('topic describe --help')).resolves.toEqual(
      expect.objectContaining({ usage: 'topic describe <topic>' }),
    );
    expect(fetch).not.toHaveBeenCalled();
  });
});
