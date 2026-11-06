import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { rejectSession } from './auth';
import { assertOk } from './api';
import { listTopics, deleteTopic } from './topics-api';
import { listGroups, deleteGroup, listShareGroups } from './groups-api';

vi.mock('./auth', () => ({ rejectSession: vi.fn(), withAuthHeaders: () => new Headers() }));

describe('assertOk session recovery', () => {
  beforeEach(() => vi.clearAllMocks());
  afterEach(() => vi.unstubAllGlobals());

  it('ends the session on an authentication failure and preserves the error', async () => {
    const response = Response.json({ error: { message: 'invalid session token' } }, { status: 401 });
    await expect(assertOk(response, 'read')).rejects.toThrow('invalid session token');
    expect(rejectSession).toHaveBeenCalledOnce();
  });

  it('does not end a valid read-only session on a forbidden mutation', async () => {
    await expect(assertOk(new Response(null, { status: 403 }), 'write')).rejects.toThrow('write failed with 403');
    expect(rejectSession).not.toHaveBeenCalled();
  });

  it('accepts successful responses without ending the session', async () => {
    await expect(assertOk(new Response(), 'read')).resolves.toBeUndefined();
    expect(rejectSession).not.toHaveBeenCalled();
  });

  it.each([
    ['topics', () => listTopics()],
    ['groups', () => listGroups()],
    ['share groups', () => listShareGroups()],
    ['delete topic', () => deleteTopic('orders')],
    ['delete group', () => deleteGroup('readers')],
  ])('uses session recovery for %s requests', async (_name, request) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 401 })));
    await expect(request()).rejects.toThrow('failed with 401');
    expect(rejectSession).toHaveBeenCalledOnce();
  });
});
