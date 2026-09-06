import { describe, expect, it, vi } from 'vitest';
import { AdminPool, type KafkaClientFactory } from './admin-pool';
import { createFakeAdmin } from './create-fake-admin';

function fakeFactory(): {
  factory: KafkaClientFactory;
  connects: ReturnType<typeof vi.fn>;
  disconnects: ReturnType<typeof vi.fn>;
} {
  const connects = vi.fn(async () => {});
  const disconnects = vi.fn(async () => {});
  const factory: KafkaClientFactory = () => ({
    admin: () => createFakeAdmin({ connect: connects, disconnect: disconnects }),
  });
  return { factory, connects, disconnects };
}

describe('AdminPool', () => {
  it('builds and connects a client on the first get() for a profile', async () => {
    const { factory, connects } = fakeFactory();
    const pool = new AdminPool(factory);

    await pool.get('staging');
    expect(connects).toHaveBeenCalledOnce();
  });

  it('reuses the same connecting admin for repeated calls with the same profile', async () => {
    const created = vi.fn();
    const factory: KafkaClientFactory = (profileName) => {
      created(profileName);
      return { admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {} }) };
    };
    const pool = new AdminPool(factory);

    const [first, second] = await Promise.all([pool.get('staging'), pool.get('staging')]);
    expect(first).toBe(second);
    expect(created).toHaveBeenCalledOnce();
  });

  it('pools null and a named profile separately', async () => {
    const created = vi.fn();
    const factory: KafkaClientFactory = (profileName) => {
      created(profileName);
      return { admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {} }) };
    };
    const pool = new AdminPool(factory);

    await pool.get(null);
    await pool.get('staging');
    expect(created).toHaveBeenCalledTimes(2);
    expect(created).toHaveBeenCalledWith(null);
    expect(created).toHaveBeenCalledWith('staging');
  });

  it('forgets a profile whose connect() rejected, so the next get() retries', async () => {
    let attempt = 0;
    const factory: KafkaClientFactory = () => ({
      admin: () =>
        createFakeAdmin({
          connect: async () => {
            attempt += 1;
            if (attempt === 1) throw new Error('connect failed');
          },
          disconnect: async () => {},
        }),
    });
    const pool = new AdminPool(factory);

    await expect(pool.get('staging')).rejects.toThrow('connect failed');
    await pool.get('staging');
    expect(attempt).toBe(2);
  });

  it('invalidate() disconnects and forgets a pooled admin', async () => {
    const { factory, disconnects } = fakeFactory();
    const pool = new AdminPool(factory);

    await pool.get('staging');
    await pool.invalidate('staging');
    expect(disconnects).toHaveBeenCalledOnce();

    const { factory: factory2, connects: connects2 } = fakeFactory();
    const pool2 = new AdminPool(factory2);
    await pool2.get('staging');
    await pool2.invalidate('staging');
    await pool2.get('staging');
    expect(connects2).toHaveBeenCalledTimes(2);
  });

  it('invalidate() on a profile that was never pooled is a no-op', async () => {
    const { factory } = fakeFactory();
    const pool = new AdminPool(factory);
    await expect(pool.invalidate('never-pooled')).resolves.toBeUndefined();
  });

  it('disposeAll() disconnects every pooled admin and clears the pool', async () => {
    const { factory, disconnects } = fakeFactory();
    const pool = new AdminPool(factory);

    await pool.get(null);
    await pool.get('staging');
    await pool.disposeAll();
    expect(disconnects).toHaveBeenCalledTimes(2);
  });
});
