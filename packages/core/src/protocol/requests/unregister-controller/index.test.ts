import { describe, expect, it } from 'vitest';
import { UnregisterController } from './index';

describe('protocol/requests/unregister-controller', () => {
  it('registers version 0', () => {
    expect(UnregisterController.versions).toEqual([0]);
  });

  it('builds a v0 request for the controller id', () => {
    const { request } = UnregisterController.protocol({ version: 0 })({ controllerId: 3001 });
    expect(request.apiKey).toBe(94);
    expect(request.apiVersion).toBe(0);
  });
});
