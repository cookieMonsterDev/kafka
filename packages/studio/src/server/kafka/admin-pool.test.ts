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
});
