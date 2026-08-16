import { describe, expect, it } from 'vitest';
import { DeleteGroups } from './index';

describe('protocol/requests/delete-groups', () => {
  it('implements versions 0 through 2', () => {
    expect(DeleteGroups.versions).toEqual([0, 1, 2]);
  });

  it('builds a request for the requested version', () => {
    const { request } = DeleteGroups.protocol({ version: 1 })({ groupIds: ['g'] });
    expect(request.apiVersion).toBe(1);
  });
});
