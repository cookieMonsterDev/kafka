import { describe, expect, it, vi } from 'vitest';
import { createShareGroupsApi } from './share-groups';

describe('admin/share-groups', () => {
  const context = {
    cluster: {
      findGroupCoordinator: vi.fn(),
      refreshMetadata: vi.fn(),
    },
    logger: { warn: vi.fn(), namespace: () => ({ warn: vi.fn() }) },
    rootLogger: { warn: vi.fn() },
    retry: { retries: 0 },
  };

  it('rejects invalid group ids for describeShareGroups', async () => {
    const api = createShareGroupsApi(context as never);
    await expect(api.describeShareGroups([''])).rejects.toThrow('non-empty strings');
  });

  it('rejects empty groups for listShareGroupOffsets', async () => {
    const api = createShareGroupsApi(context as never);
    await expect(api.listShareGroupOffsets({ groups: [] })).rejects.toThrow('Invalid groups array');
  });
});
