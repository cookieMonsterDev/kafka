import { describe, expect, it, vi } from 'vitest';
import { ProfileClientPool } from './profile-client-pool';

function client() {
  return { connect: vi.fn(async () => {}), disconnect: vi.fn(async () => {}) };
}

describe('ProfileClientPool', () => {
  it('shares a pending connection, including its resulting client', async () => {
    const created = client();
    const factory = vi.fn(() => created);
    const pool = new ProfileClientPool(factory);
    const first = pool.get('staging');
    expect(pool.get('staging')).toBe(first);
    expect(await first).toBe(created);
    expect(factory).toHaveBeenCalledOnce();
  });

  it('keeps the direct profile separate from a profile named __default__', async () => {
    const factory = vi.fn(() => client());
    const pool = new ProfileClientPool(factory);
    expect(await pool.get(null)).not.toBe(await pool.get('__default__'));
    expect(factory).toHaveBeenCalledWith(null);
    expect(factory).toHaveBeenCalledWith('__default__');
  });

  it('cleans up a failed connection and retries without masking its error', async () => {
    const created = client();
    created.connect.mockRejectedValueOnce(new Error('connection failed'));
    created.disconnect.mockRejectedValueOnce(new Error('cleanup failed'));
    const pool = new ProfileClientPool(() => created);
    await expect(pool.get(null)).rejects.toThrow('connection failed');
    expect(created.disconnect).toHaveBeenCalledOnce();
    await expect(pool.get(null)).resolves.toBe(created);
    expect(created.connect).toHaveBeenCalledTimes(2);
  });

  it('does not let an invalidated connection failure evict its replacement', async () => {
    let rejectConnection = (_error: Error) => {};
    const pending = new Promise<void>((_resolve, reject) => {
      rejectConnection = reject;
    });
    const old = client();
    old.connect.mockImplementation(() => pending);
    const replacement = client();
    const pool = new ProfileClientPool(vi.fn().mockReturnValueOnce(old).mockReturnValue(replacement));
    const first = pool.get('staging');
    const invalidated = pool.invalidate('staging');
    const next = pool.get('staging');
    await next;
    rejectConnection(new Error('old connection failed'));
    await expect(first).rejects.toThrow('old connection failed');
    await invalidated;
    expect(pool.get('staging')).toBe(next);
    expect(old.disconnect).toHaveBeenCalledOnce();
    expect(replacement.disconnect).not.toHaveBeenCalled();
  });

  it('disconnects a pending invalidated connection once it succeeds', async () => {
    let finishConnecting = () => {};
    const pending = new Promise<void>((resolve) => {
      finishConnecting = resolve;
    });
    const created = client();
    created.connect.mockImplementation(() => pending);
    const pool = new ProfileClientPool(() => created);
    const first = pool.get(null);
    const invalidated = pool.invalidate(null);
    finishConnecting();
    await first;
    await invalidated;
    expect(created.disconnect).toHaveBeenCalledOnce();
    await pool.get(null);
    expect(created.connect).toHaveBeenCalledTimes(2);
  });

  it('invalidates unknown profiles without creating a client', async () => {
    const factory = vi.fn(() => client());
    await new ProfileClientPool(factory).invalidate('unknown');
    expect(factory).not.toHaveBeenCalled();
  });

  it('disconnects all entries on shutdown and clears their cache', async () => {
    const clients = [client(), client()];
    const factory = vi.fn().mockReturnValueOnce(clients[0]).mockReturnValue(clients[1]);
    const pool = new ProfileClientPool(factory);
    await Promise.all([pool.get(null), pool.get('staging')]);
    await pool.disposeAll();
    for (const entry of clients) expect(entry.disconnect).toHaveBeenCalledOnce();
    await pool.get(null);
    expect(factory).toHaveBeenCalledTimes(3);
  });
});
