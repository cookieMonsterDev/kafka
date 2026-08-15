import { describe, expect, it } from 'vitest';
import { FindCoordinator } from './index.js';

describe('protocol/requests/find-coordinator', () => {
  it('implements versions 0 through 2', () => {
    expect(FindCoordinator.versions).toEqual([0, 1, 2]);
  });

  it('accepts groupId on v0', () => {
    const { request } = FindCoordinator.protocol({ version: 0 })({ groupId: 'g' });
    expect(request.apiVersion).toBe(0);
  });

  it('accepts coordinatorKey/coordinatorType on v1+', () => {
    const { request } = FindCoordinator.protocol({ version: 1 })({ coordinatorKey: 'g', coordinatorType: 1 });
    expect(request.apiVersion).toBe(1);
  });
});
