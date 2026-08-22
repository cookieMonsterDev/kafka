import { describe, expect, it } from 'vitest';
import { UnregisterBroker } from './index';

describe('protocol/requests/unregister-broker', () => {
  it('registers version 0', () => {
    expect(UnregisterBroker.versions).toEqual([0]);
  });

  it('builds a v0 request for the broker id', () => {
    const { request } = UnregisterBroker.protocol({ version: 0 })({ brokerId: 2 });
    expect(request.apiVersion).toBe(0);
  });
});
